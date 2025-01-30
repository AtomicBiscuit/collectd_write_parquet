/**
 * collectd - src/write_file_test.c
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 **/

extern "C" {
#include "collectd.h"

#include "plugin.h"
#include "utils/common/common.h"

#include "utils/strbuf/strbuf.h"

#include <fcntl.h>
#include <stdio.h>
}

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <utility>
#include <parquet/arrow/writer.h>
#include <parquet/stream_writer.h>

#define LOG_AND_RETURN_ON_ERROR(e, msg, ...)                                   \
  do {                                                                         \
    int code = static_cast<int>((e));                                          \
    if (code != 0) {                                                           \
      P_ERROR((std::string((msg)) + ": %i").c_str(), __VA_ARGS__, code);       \
      return code;                                                             \
    }                                                                          \
  } while (0)


using std::chrono::system_clock;

using time_point = system_clock::time_point;
using node_shared_ptr = std::shared_ptr<parquet::schema::GroupNode>;

static const char *config_keys[] = {"basedir", "fileduration", "compression", "buffersize", "bufferduration"};
static int config_keys_num = STATIC_ARRAY_SIZE(config_keys);


static const inline std::string filename = "active.parquet";
static std::chrono::seconds file_duration = std::chrono::seconds(3600);
static std::chrono::seconds buffer_duration = std::chrono::seconds(300);
static uint64_t buffer_capacity = 10000;
static std::atomic<uint64_t> buffer_size = 0;

static parquet::WriterProperties::Builder properties_builder{};

static node_shared_ptr schema_int = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make(
                "integer",
                parquet::Repetition::OPTIONAL,
                {parquet::schema::PrimitiveNode::Make(
                        "value int",
                        parquet::Repetition::OPTIONAL,
                        parquet::Type::INT64,
                        parquet::ConvertedType::INT_64
                )}
        )
);

static node_shared_ptr schema_double = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make(
                "double",
                parquet::Repetition::OPTIONAL,
                {parquet::schema::Double("value double")}
        )
);

static std::string wf_time_point_to_string(time_point point,
                                           const std::string &format) {
    tm time_tm = {0};
    char time_buf[100] = {};

    time_t now = system_clock::to_time_t(point);
    localtime_r(&now, &time_tm);
    strftime(time_buf, sizeof(time_buf), format.c_str(), &time_tm);

    return time_buf;
}

namespace {
enum class MetricValueType {
    DOUBLE,
    INT64,
    NONE,
};

class File {
private:
    std::filesystem::path path{};
    std::string path_str{};

    time_point creation_time{};
    std::shared_ptr<arrow::io::FileOutputStream> file{};
public:
    File(const std::filesystem::path &path) :
            path(path), path_str((path / filename).string()),
            creation_time(system_clock::now()) {
        recreate();
    };

    bool is_active() {
        return system_clock::now() - creation_time < file_duration;
    }

    int recreate() {
        if (file and not file->closed()) {
            LOG_AND_RETURN_ON_ERROR(file->Close().code(), "file closing (%s) failed",
                                    path_str.c_str());
            std::string time_str =
                    wf_time_point_to_string(creation_time, "%Y%m%dT%H%M%S.parquet");

            std::error_code error_code{};
            std::filesystem::rename(path_str, path / time_str, error_code);
            LOG_AND_RETURN_ON_ERROR(error_code.value(), "file renaming (%s) failed",
                                    path_str.c_str());
        }
        auto res = arrow::io::FileOutputStream::Open(path_str, false);
        LOG_AND_RETURN_ON_ERROR(res.status().code(), "file opening (%s) failed",
                                path_str.c_str());
        file = std::move(res.ValueOrDie());

        creation_time = system_clock::now();
        return 0;
    }

    std::shared_ptr<arrow::io::FileOutputStream> stream() { return file; }
};

class IWriter {
public:
    virtual void flush() = 0;

    virtual int write(std::variant<int64_t, double>) = 0;

    virtual ~IWriter() = default;
};

template<typename DataType>
class Writer : public IWriter {
private:
    File file;
    parquet::StreamWriter writer;
    node_shared_ptr schema;
    std::mutex mut;

    time_point buffer_flush_time{};
    std::vector<DataType> buffer{};
public:
    Writer(const std::filesystem::path &path, const node_shared_ptr &schema) :
            file(path), schema(schema), buffer_flush_time(system_clock::now()) {
        writer = parquet::StreamWriter{
                parquet::ParquetFileWriter::Open(file.stream(), schema, properties_builder.build())
        };
    };

    bool is_buffer_active() {
        return buffer_size < buffer_capacity and system_clock::now() - buffer_flush_time < buffer_duration;
    }

    void flush() override {
        for (DataType value: buffer) {
            writer << value << parquet::EndRow;
        }
        P_WARNING("flush:  %lu", buffer.size());
        buffer_size -= buffer.size();
        buffer_flush_time = system_clock::now();
        buffer.clear();
    }

    int write(std::variant<int64_t, double> raw_data) override {
        std::lock_guard lock(mut);
        DataType data = std::get<DataType>(raw_data);
        if (not file.is_active()) {
            P_WARNING(".................................................................RECREATING");
            flush();
            writer = parquet::StreamWriter{};
            if (int err = file.recreate()) {
                return err;
            }
            writer = parquet::StreamWriter{
                    parquet::ParquetFileWriter::Open(file.stream(), schema, properties_builder.build())
            };
        }
        if (not is_buffer_active()) {
            if (system_clock::now() - buffer_flush_time > buffer_duration) {
                P_WARNING(".................................................................buffer has exceed!!!!!!");
            }
            flush();
        }
        buffer.push_back(data);
        buffer_size++;
        return 0;
    }
};

class Director {
    std::map<std::string, std::shared_ptr<IWriter>> dirs{};
    std::filesystem::path base_dir{};

public:
    Director() = default;

    void set_path(const std::string &path) { base_dir = path; }

    template<typename DataType>
    std::shared_ptr<IWriter> get(const std::string &name, const node_shared_ptr &schema) {
        if (dirs.find(name) != dirs.end()) {
            return dirs.at(name);
        }
        std::error_code error_code{};
        std::filesystem::create_directories(base_dir / name, error_code);
        if (error_code) {
            P_ERROR("directory creating (%s) error: %s", (base_dir / name).c_str(),
                    error_code.message().c_str());
        }
        dirs.emplace(name, std::make_shared<Writer<DataType>>(base_dir / name, schema));
        return dirs.at(name);
    }
};
} // namespace

static Director handler{};

static double wf_parse_metric_double(const metric_t *mt) {
    switch (mt->family->type) {
        case METRIC_TYPE_GAUGE:
            return mt->value.gauge;
        case METRIC_TYPE_COUNTER_FP:
            return mt->value.counter_fp;
        case METRIC_TYPE_UP_DOWN_FP:
            return mt->value.up_down_fp;
        default:
            break;
    }
    return 0;
}

static int64_t wf_parse_metric_int(const metric_t *mt) {
    switch (mt->family->type) {
        case METRIC_TYPE_COUNTER:
            return mt->value.counter;
        case METRIC_TYPE_UP_DOWN:
            return mt->value.up_down;
        default:
            break;
    }
    return 0;
}

static MetricValueType wf_get_metric_type(const metric_t *mt) {
    switch (mt->family->type) {
        case METRIC_TYPE_GAUGE:
            return MetricValueType::DOUBLE;
        case METRIC_TYPE_COUNTER:
            return MetricValueType::INT64;
        case METRIC_TYPE_COUNTER_FP:
            return MetricValueType::DOUBLE;
        case METRIC_TYPE_UP_DOWN:
            return MetricValueType::INT64;
        case METRIC_TYPE_UP_DOWN_FP:
            return MetricValueType::DOUBLE;
        default:
            break;
    }
    return MetricValueType::NONE;
}

static int wf_write_callback(metric_family_t const *fam,
                             user_data_t *user_data) {
    auto host = label_set_get(fam->resource, "host.name");
    if (not host) {
        P_ERROR("Expected host as metric family resource");
        return ENOENT;
    }
    std::filesystem::path base;
    std::string_view tmp = host;
    while (!tmp.empty() and tmp.back() == '.') {
        tmp.remove_suffix(1);
    }
    base /= tmp;
    base /= fam->name;
    for (size_t i = 0; i < fam->metric.num; i++) {
        metric_t *mt = fam->metric.ptr + i;
        std::filesystem::path full_path = base;
        for (size_t j = 0; j < mt->label.num; j++) {
            label_pair_t *lab = mt->label.ptr + j;
            full_path /= lab->value;
        }
        MetricValueType type = wf_get_metric_type(mt);
        if (type == MetricValueType::DOUBLE) {
            auto writer = handler.get<double>(full_path.string(), schema_double);
            writer->write(wf_parse_metric_double(mt));
        } else if (type == MetricValueType::INT64) {
            auto writer = handler.get<int64_t>(full_path.string(), schema_int);
            writer->write(wf_parse_metric_int(mt));
        }
    }
    P_WARNING("BUFF CAP: %lu, SIZE: %lu, REMAIN %lu", buffer_capacity, buffer_size.load(),
              buffer_capacity - buffer_size);
    return 0;
}

static int wf_config_callback(const char *key, const char *value) {
    if (strcasecmp("basedir", key) == 0) {
        handler.set_path(value);
    } else if (strcasecmp("fileduration", key) == 0) {
        file_duration = std::chrono::seconds(std::strtoul(value, nullptr, 10));
    } else if (strcasecmp("bufferduration", key) == 0) {
        buffer_duration = std::chrono::seconds(std::strtoul(value, nullptr, 10));
    } else if (strcasecmp("buffersize", key) == 0) {
        buffer_capacity = std::strtoul(value, nullptr, 10);
    } else if (strcasecmp("compression", key) == 0) {
        if (strcasecmp("uncompressed", value) == 0 or strcasecmp("off", value) == 0) {
            properties_builder.compression(parquet::Compression::UNCOMPRESSED);
        } else if (strcasecmp("brotli", value) == 0) {
            properties_builder.compression(parquet::Compression::BROTLI);
        } else if (strcasecmp("gzip", value) == 0) {
            properties_builder.compression(parquet::Compression::GZIP);
        } else if (strcasecmp("zstd", value) == 0) {
            properties_builder.compression(parquet::Compression::ZSTD);
        } else {
            P_ERROR("Invalid compression type (%s)", value);
            return EINVAL;
        }
    } else {
        P_ERROR("Invalid configuration option (%s)", key);
        return -EINVAL;
    }
    return 0;
}

extern "C" {
void module_register(void) {
    plugin_register_config("write_file_test", wf_config_callback, config_keys,
                           config_keys_num);
    plugin_register_write("write_file_test", wf_write_callback, NULL);
}
}