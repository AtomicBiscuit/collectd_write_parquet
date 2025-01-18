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
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <parquet/arrow/writer.h>
#include <parquet/stream_writer.h>

#define LOG_AND_RETURN_ON_ERROR(s, msg, ...)                                   \
  int state = static_cast<int>((s));                                           \
  if (state != 0) {                                                            \
    P_ERROR((std::string((msg)) + ": %i").c_str(), __VA_ARGS__, state);        \
    return state;                                                              \
  }                                                                            \
  }                                                                            \
  while (0)

static const char *config_keys[] = {"BaseDir", "Duration"};
static int config_keys_num = STATIC_ARRAY_SIZE(config_keys);

namespace {
class Directory {
private:
  static const inline std::string base_name = "active.parquet";
  std::filesystem::path path{};

  std::chrono::seconds delta;
  std::chrono::system_clock::time_point creation_time{};

  std::shared_ptr<arrow::io::FileOutputStream> file_handler{};
  parquet::StreamWriter writer;
  std::shared_ptr<parquet::schema::GroupNode> schema =
      std::static_pointer_cast<parquet::schema::GroupNode>(
          parquet::schema::GroupNode::Make("schema",
                                           parquet::Repetition::REQUIRED,
                                           {parquet::schema::Double("value")}));

  std::mutex mut;

public:
  static inline parquet::WriterProperties::Builder builder{};

  Directory(std::filesystem::path path, std::chrono::seconds delta)
      : path(std::move(path)), delta(delta),
        creation_time(std::chrono::system_clock::now()) {
    recreate_();
  };

  Directory(const Directory &other)
      : path(other.path), delta(other.delta),
        creation_time(std::chrono::system_clock::now()) {
    recreate_();
  }

  Directory &operator=(const Directory &other) {
    if (&other == this) {
      return *this;
    }
    if (file_handler) {
      PARQUET_IGNORE_NOT_OK(file_handler->Close());
    }
    path = other.path;
    delta = other.delta;
    return *this;
  }

  int write(double data) {
    std::lock_guard lock(mut);
    if (std::chrono::system_clock::now() - creation_time > delta) {
      if (int err = recreate_()) {
        return err;
      }
      creation_time = std::chrono::system_clock::now();
    }
    // P_WARNING("%i %li %i", writer.current_column(), writer.current_row(),
    //           writer.num_columns());
    writer << data << parquet::EndRow;
    // writer << parquet::EndRowGroup;
    //  P_WARNING("data: %f", data);
    // PARQUET_IGNORE_NOT_OK(file_handler->Flush());
    return 0;
  }

private:
  int recreate_() {
    if (file_handler and not file_handler->closed()) {
      // writer.~StreamWriter();
      writer = parquet::StreamWriter();
      LOG_AND_RETURN_ON_ERROR(file_handler->Close().code(),
                              "file closing (%s) failed",
                              (path / base_name).c_str());

      tm time_tm = {0};
      char time_buf[28];

      time_t now = std::chrono::system_clock::to_time_t(creation_time);
      localtime_r(&now, &time_tm);
      strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H.%M.%S.parquet",
               &time_tm);

      std::error_code er{};
      std::filesystem::rename(path / base_name, path / time_buf, er);
      LOG_AND_RETURN_ON_ERROR(er.value(), "file renaming (%s) failed",
                              (path / base_name).c_str());
    }
    auto res = arrow::io::FileOutputStream::Open(path / base_name, false);
    LOG_AND_RETURN_ON_ERROR(res.status().code(), "file opening (%s) failed",
                            (path / base_name).c_str());
    file_handler = std::move(res.ValueOrDie());

    // builder.compression(parquet::Compression::BROTLI);
    writer = parquet::StreamWriter{parquet::ParquetFileWriter::Open(
        file_handler,
        std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make(
                "schema", parquet::Repetition::REQUIRED,
                {parquet::schema::Double("value")})),
        builder.build())};
    return 0;
  }
};

class DirectoryHandler {
  std::map<std::string, Directory> dirs_{};
  std::filesystem::path base_dir{};
  std::chrono::seconds delta{};

public:
  DirectoryHandler() = default;

  void set_path(const std::string &path) { base_dir = path; }

  void set_delta(int seconds) { delta = std::chrono::seconds(seconds); }

  Directory &get(const std::string &name) {
    if (dirs_.find(name) != dirs_.end()) {
      return dirs_.at(name);
    }
    std::error_code err{};
    std::filesystem::create_directories(base_dir / name, err);
    if (err) {
      P_ERROR("%s", err.message().c_str());
    }
    dirs_.emplace(name, Directory(base_dir / name, delta));
    return dirs_.at(name);
  }
};
} // namespace

static DirectoryHandler dirs{};
static enum class StreamType { file, out, err } stream_type = StreamType::out;

static double wf_parse_metric(metric_t *mt) {
  switch (mt->family->type) {
  case METRIC_TYPE_GAUGE:
    return mt->value.gauge;
  case METRIC_TYPE_COUNTER:
    return static_cast<double>(mt->value.counter);
  case METRIC_TYPE_COUNTER_FP:
    return mt->value.counter_fp;
  case METRIC_TYPE_UP_DOWN:
    return static_cast<double>(mt->value.up_down);
  case METRIC_TYPE_UP_DOWN_FP:
    return mt->value.up_down_fp;
  case METRIC_TYPE_UNTYPED:
    break;
  }
  return NAN;
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
      wf_parse_metric(mt);
      dirs.get(full_path.string()).write(wf_parse_metric(mt));
    }
  }
  return 0;
}

static int wf_config_callback(const char *key, const char *value) {
  if (strcasecmp("BaseDir", key) == 0) {
    if (strcasecmp(value, "stdout") == 0) {
      stream_type = StreamType::out;
    } else if (strcasecmp(value, "stderr") == 0) {
      stream_type = StreamType::err;
    } else {
      stream_type = StreamType::file;
      dirs.set_path(value);
    }
  } else if (strcasecmp("Duration", key) == 0) {
    dirs.set_delta(std::strtoul(value, nullptr, 10));
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