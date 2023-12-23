import unittest
from unittest import mock
from opentelemetry import trace
from opentelemetry.instrumentation.dbt import _instrument

class TestDbtInstrumentation(unittest.TestCase):
    def setUp(self):
        self.tracer_provider = trace.DefaultTracerProvider()
        self.tracer = self.tracer_provider.get_tracer(__name__)
        self.include_db_statement = False

    @mock.patch("opentelemetry.trace.get_tracer")
    @mock.patch("opentelemetry.instrumentation.dbt.wrap_function_wrapper")
    def test_instrument(self, mock_wrap_function_wrapper, mock_get_tracer):
        mock_get_tracer.return_value = self.tracer

        _instrument(self.tracer_provider, self.include_db_statement)

        mock_get_tracer.assert_called_once_with(
            __name__,
            __version__,
            self.tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        calls = [
            mock.call("dbt.task", "ModelRunner.execute", mock.ANY),
            mock.call("dbt.task", "RunTask.run_hooks", mock.ANY),
            mock.call("dbt.adapters.base", "BaseAdapter.execute", mock.ANY),
        ]
        mock_wrap_function_wrapper.assert_has_calls(calls, any_order=True)

    @mock.patch("opentelemetry.trace.Span.is_recording")
    def test_wrap_traced(self, mock_is_recording):
        mock_is_recording.return_value = True

        func = mock.Mock()
        instance = mock.Mock()
        args = ("arg1", "arg2")
        kwargs = {"kwarg1": "value1"}

        with self.tracer.start_as_current_span("test") as span:
            _wrap_traced = _instrument(self.tracer_provider, self.include_db_statement)
            result = _wrap_traced(func, instance, args, kwargs)

        func.assert_called_once_with(*args, **kwargs)
        self.assertEqual(result, func.return_value)
        mock_is_recording.assert_called_once()

if __name__ == "__main__":
    unittest.main()
