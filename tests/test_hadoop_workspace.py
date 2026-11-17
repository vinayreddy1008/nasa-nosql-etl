from pipelines.mapreduce import backend


def test_hadoop_worker_paths(monkeypatch):
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

    expected = [
        "python3 /workspace/pipelines/mapreduce/mapper.py",
        "python3 /workspace/pipelines/mapreduce/reducer.py",
        "python3 /workspace/pipelines/mapreduce/top_mapper.py",
        "python3 /workspace/pipelines/mapreduce/top_reducer.py",
    ]

    for command, program in zip(
        [commands[0], commands[0], commands[1], commands[1]],
        expected,
    ):
        assert program in command

    assert all(
        c.index("-files ") < c.index("-input ")
        for c in commands
    )
