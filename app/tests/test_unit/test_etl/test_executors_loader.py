from collections import defaultdict

from internal.etl.executors import ETLLoaderExecutor


class MockETLLoader:

    def __init__(self):
        self.methods = defaultdict(list)

    def run(self, *args, **kwargs):
        self.methods["run"].append((args, kwargs))


class TestCaseUnitETLExecutorsLoader:

    def test_run_called_transformer_run(self):
        executor = ETLLoaderExecutor(
            loader=MockETLLoader(),
        )
        executor.run()

        mocked_loader = executor._loader
        assert len(mocked_loader.methods["run"]) == 1
        assert mocked_loader.methods["run"][0] == ((), {})
