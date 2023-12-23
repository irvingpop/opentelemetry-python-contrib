# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

"""

from typing import Collection

import dbt.context, dbt.task, dbt.adapters.base
from wrapt import wrap_function_wrapper

from opentelemetry import trace
from opentelemetry.instrumentation.dbt.package import _instruments
from opentelemetry.instrumentation.dbt.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import SpanAttributes
import sys


def _instrument(tracer_provider, include_db_statement=False):
    """Instruments the cassandra-driver/scylla-driver module

    Wraps cassandra.cluster.Session.execute_async().
    """
    tracer = trace.get_tracer(
        __name__,
        __version__,
        tracer_provider,
        schema_url="https://opentelemetry.io/schemas/1.11.0",
    )

    def _wrap_generic(func, instance, args, kwargs):
        with tracer.start_as_current_span(
            func.__qualname__, kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                for key in ["project_name", "project_root", "profile_name", "version", "threads"]:
                    span.set_attribute("config." + key, getattr(instance.config, key))

            response = func(*args, **kwargs)
            return response

    def _wrap_baseadapter(func, instance, args, kwargs):
        with tracer.start_as_current_span(
            "baseadapter-" + func.__name__, kind=trace.SpanKind.CLIENT
        ) as span:
            if span.is_recording():
                span.set_attribute(SpanAttributes.DB_STATEMENT, str(args))
                span.set_attribute(SpanAttributes.DB_SYSTEM, instance.type())
                span.set_attribute(SpanAttributes.DB_OPERATION, instance.type())
                # config attributes
                for key in ["project_name", "project_root", "profile_name", "version", "threads"]:
                    span.set_attribute("config." + key, getattr(instance.config, key))

                # Are these snowflake-specific, and what to do here for others?
                if instance.type() == "snowflake":
                    for key in ["account", "user", "database", "schema", "warehouse", "role"]:
                        span.set_attribute("config." + key, getattr(instance.config.credentials, key))

                # import ipdb; ipdb.set_trace()
                # sys.exit()

                # for key in instance.config.__dir__():
                #     if key.startswith("__"):
                #         continue
                #     if key in ["credentials", "dependencies", "args"]:
                #         continue
                #     try:
                #         print("Key: ", key, "Val: ", instance.config.__dict__[key])
                #     except:
                #         pass

            response = func(*args, **kwargs)
            return response

    wrap_function_wrapper(
        "dbt.task.base", "BaseRunner._build_run_result", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.task.base", "BaseRunner.compile_and_execute", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.task.base", "BaseRunner.safe_run", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.task.run", "RunTask.run", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.task.runnable", "GraphRunnableTask.call_runner", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.task.run", "ModelRunner.execute", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.task.run", "RunTask.run_hooks", _wrap_generic,
    )

    wrap_function_wrapper(
        "dbt.adapters.base.impl", "BaseAdapter.execute", _wrap_baseadapter,
    )


class DBTCoreInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        _instrument(
            tracer_provider=kwargs.get("tracer_provider"),
        )

    def _uninstrument(self, **kwargs):
        return

    # The unwrap function is typically used to remove a wrapper that was previously applied to a function or method. In this case, it's removing a wrapper from the execute_async method of the Session class.
    # def _uninstrument(self, **kwargs):
    #     unwrap(cassandra.cluster.Session, "execute_async")
