"""The generated gRPC code is importable and exposes the Boss service."""


def test_generated_code_imports():
    from mapreduce import mapreduce_pb2, mapreduce_pb2_grpc

    service = mapreduce_pb2.DESCRIPTOR.services_by_name["Boss"]
    assert sorted(method.name for method in service.methods) == [
        "CancelJob",
        "GetJobStatus",
        "GetTask",
        "Heartbeat",
        "ListWorkers",
        "ReportTask",
        "SubmitJob",
    ]
    assert hasattr(mapreduce_pb2_grpc, "BossStub")
    assert hasattr(mapreduce_pb2_grpc, "BossServicer")


def test_task_kinds():
    from mapreduce import mapreduce_pb2

    assert mapreduce_pb2.NONE == 0
    assert mapreduce_pb2.MAP == 1
    assert mapreduce_pb2.REDUCE == 2
