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

#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

static const char *config_keys[] = {"File", "Duration"};
static int config_keys_num = STATIC_ARRAY_SIZE(config_keys);

namespace {
class DirectoryHandler {
private:
  static const inline std::string base_name = "active";
  std::string path{};
  std::ofstream file_handler{};
  std::chrono::seconds delta;
  std::chrono::system_clock::time_point creation_time{};
  std::mutex mut;

public:
  DirectoryHandler() : path(""), delta(0) {}

  DirectoryHandler(std::string path, int delta)
      : path(path), delta(std::chrono::seconds(delta)) {}

  DirectoryHandler(const DirectoryHandler &other)
      : path(other.path), delta(other.delta) {}

  void set_path(std::string &&other) { path = std::move(other); }

  void set_delta(int other) { delta = std::chrono::seconds(other); }

  int write(const std::string &data) {
    std::lock_guard lock(mut);
    if (std::chrono::system_clock::now() - creation_time > delta) {
      creation_time = std::chrono::system_clock::now();
      if (int err = recreate_()) {
        return err;
      }
    }
    file_handler.write(data.c_str(), data.size());
    return 0;
  }

private:
  int recreate_() {
    if (file_handler.is_open()) {
      file_handler.close();

      tm time_tm = {0};
      char time_buf[20]; // "1999-01-01 12:11:10" - 19 symbols

      time_t now = time(nullptr);
      localtime_r(&now, &time_tm);
      strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H.%M.%S", &time_tm);

      if (rename((path + "/" + base_name).c_str(),
                 (path + "/" + time_buf).c_str()) != 0) {
        P_ERROR("file renaming (%s) failed: %s",
                (path + "/" + base_name).c_str(),
                std::to_string(errno).c_str());
        return EINVAL;
      }
    }

    file_handler.open(path + "/" + base_name, std::ios::app);
    if (not file_handler.is_open()) {
      P_ERROR("file opening (%s) failed: %s", (path + "/" + base_name).c_str(),
              std::to_string(errno).c_str());
      return EINVAL;
    }
    return 0;
  }
};
} // namespace

static DirectoryHandler dir{};
static enum class StreamType { file, out, err } stream_type = StreamType::out;

static int wf_write_callback(metric_family_t const *fam,
                             __attribute__((unused)) user_data_t *user_data) {
  std::stringstream stream;

  stream << "family: " << fam->name << "\n";
  stream << "resources:\n";
  for (size_t j = 0; j < fam->resource.num; j++) {
    label_pair_t *l = fam->resource.ptr + j;
    stream << "        " << l->name << ": " << l->value << "\n";
  }
  stream << "\n";

  strbuf_t buf = STRBUF_CREATE;
  tm time_tm = {0};
  char time_buf[20];

  for (size_t i = 0; i < fam->metric.num; i++) {
    metric_t *mt = fam->metric.ptr + i;

    time_t tmp = CDTIME_T_TO_TIME_T(mt->time);
    localtime_r(&tmp, &time_tm);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &time_tm);
    stream << "    (" << time_buf << ") ";

    value_marshal_text(&buf, mt->value, fam->type);
    stream << "    value " << buf.ptr << " of type "
           << (METRIC_TYPE_TO_STRING(fam->type)) << "\n";

    stream << "    labels:\n";

    strbuf_reset(&buf);

    for (size_t j = 0; j < mt->label.num; j++) {
      label_pair_t *l = mt->label.ptr + j;
      stream << "        " << l->name << " " << l->value << "\n";
    }
    stream << "\n";
  }
  STRBUF_DESTROY(buf);

  dir.write(stream.str());
  return 0;
}

static int wf_config_callback(const char *key, const char *value) {
  if (strcasecmp("File", key) == 0) {
    if (strcasecmp(value, "stdout") == 0) {
      stream_type = StreamType::out;
    } else if (strcasecmp(value, "stderr") == 0) {
      stream_type = StreamType::err;
    } else {
      stream_type = StreamType::file;
      dir.set_path(value);
    }
  } else if (strcasecmp("Duration", key) == 0) {
    dir.set_delta(std::strtoul(value, NULL, 10));
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