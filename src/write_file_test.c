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

#include "collectd.h"

#include "plugin.h"
#include "utils/common/common.h"

#include "utils/strbuf/strbuf.h"

#include <fcntl.h>

static const char* config_keys[] = { "File" };
static int config_keys_num = STATIC_ARRAY_SIZE(config_keys);

static char* file_name = NULL;
static pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;

static enum stream_e {
  eFILE,
  eSTDOUT,
  eSTDERR
} stream_type;

static int wf_write_callback(metric_family_t const* fam, __attribute__((unused)) user_data_t* user_data) {
  pthread_mutex_lock(&file_lock);

  FILE* stream;

  switch (stream_type) {
  case (eFILE):
    stream = fopen(file_name, "a");
    break;
  case(eSTDOUT):
    stream = stdout;
    break;
  case(eSTDERR):
    stream = stderr;
    break;
  default:
    stream = NULL;
  }

  if (stream == NULL) {
    P_ERROR("fopen (%s) failed: %s", file_name, STRERRNO);
    pthread_mutex_unlock(&file_lock);
    return EINVAL;
  }

  fprintf(stream, "family: %s ----  %s\n    resources:\n", fam->name, fam->help);
  for (int j = 0; j < fam->resource.num; j++) {
    label_pair_t* l = fam->resource.ptr + j;
    fprintf(stream, "        %s: %s\n", l->name, l->value);
  }

  strbuf_t buf = STRBUF_CREATE;
  struct tm time_tm = { 0 };
  char time_buf[20];  // "1999-01-01 12:11:10" - 19 symbols

  for (int i = 0; i < fam->metric.num; i++) {
    metric_t* mt = fam->metric.ptr + i;

    localtime_r(&CDTIME_T_TO_TIME_T(mt->time), &time_tm);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &time_tm);
    fprintf(stream, "    time: %s\n", time_buf);

    value_marshal_text(&buf, mt->value, fam->type);
    fprintf(stream, "    value %s%s of type [%s]\n", buf.ptr, fam->unit, METRIC_TYPE_TO_STRING(fam->type));
    fprintf(stream, "    labels:\n");

    strbuf_reset(&buf);

    for (int j = 0; j < mt->label.num; j++) {
      label_pair_t* l = mt->label.ptr + j;
      fprintf(stream, "        %s: %s\n", l->name, l->value);
    }
    fprintf(stream, "        \n");
  }
  STRBUF_DESTROY(buf);

  if (stream_type == eFILE) {
    fclose(stream);
  } else {
    fflush(stream);
  }

  pthread_mutex_unlock(&file_lock);
  return 0;
}

static int wf_config_callback(const char* key, const char* value) {
  if (strcasecmp("File", key) != 0) {
    P_ERROR("Invalid configuration option (%s)", key);
    return -EINVAL;
  }

  free(file_name);
  file_name = sstrdup(value);

  if (strcasecmp(file_name, "stdout") == 0) {
    stream_type = eSTDOUT;
  } else if (strcasecmp(file_name, "stderr") == 0) {
    stream_type = eSTDERR;
  } else {
    stream_type = eFILE;

    FILE* stream = fopen(file_name, "a");
    if (stream == NULL) {
      P_ERROR("fopen (%s) failed: %s", file_name, STRERRNO);
      return -EINVAL;
    }
    fclose(stream);
  }

  return 0;
}

void module_register(void) {
  plugin_register_config("write_file_test", wf_config_callback, config_keys, config_keys_num);
  plugin_register_write("write_file_test", wf_write_callback, NULL);
}
