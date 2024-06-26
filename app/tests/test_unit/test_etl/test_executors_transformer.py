from collections import defaultdict
from unittest.mock import patch

from internal.etl.executors import ETLTransformerExecutor
from internal.etl.transformers import ETLTransformerInterface


class MockETLTransformer:

    def __init__(self):
        self.methods = defaultdict(list)

    def run(self, *args, **kwargs):
        self.methods["run"].append((args, kwargs))


class MockETLPipeline:

    def __init__(self):
        self.methods = defaultdict(list)

    def send(self, *args, **kwargs):
        self.methods["send"].append((args, kwargs))


class TestCaseUnitETLExecutorsTransformer:

    def test_run_called_transformer_run(self):
        executor = ETLTransformerExecutor(
            transformer=MockETLTransformer(),
            pipeline=MockETLPipeline()
        )
        executor.run()

        mocked_transformer = executor._transformer
        assert len(mocked_transformer.methods["run"]) == 1
        assert mocked_transformer.methods["run"][0] == ((), {})

    def test_run_called_pipeline(self):
        with patch("internal.etl.transformers.ETLTransformerInterface.run") as mock:
            mock.return_value = [{
                "sample": "value",
                "sample2": "value2"
            }]
            executor = ETLTransformerExecutor(
                transformer=ETLTransformerInterface(),
                pipeline=MockETLPipeline()
            )
            executor.run()

            mocked_pipeline = executor._pipeline
            assert len(mocked_pipeline.methods["send"]) == 1
            assert mocked_pipeline.methods["send"][0] == (([{
                "sample": "value",
                "sample2": "value2"
            }],), {})
