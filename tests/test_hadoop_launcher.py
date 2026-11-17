from pipelines.mapreduce import backend

def test_hadoop_option_order(monkeypatch):
    commands = []
    monkeypatch.setattr(backend, "_run", commands.append)

    backend._job(
        ["data/NASA_access_log_Jul95"],
        "work/test_stage1",
        "mapper.py",
        "reducer.py",
        [
            "pipelines/mapreduce/mapper.py",
            "pipelines/mapreduce/reducer.py",
            "common/spec.py",
        ],
        100000,
    )

    backend._job(
        ["work/test_stage1"],
        "work/test_stage2",
        "top_mapper.py",
        "top_reducer.py",
        [
            "pipelines/mapreduce/top_mapper.py",
            "pipelines/mapreduce/top_reducer.py",
        ],
    )

    for command in commands:
        assert command.index("-files ") < command.index("-input ")

    assert commands[0].index("-files ") < commands[0].index("-inputformat ")
