"""Public benchmark evaluation sets: LongMemEval + LoCoMo (docs/4-记忆管理
评测集设计.md §二/§三).

Raw benchmark data is downloaded into evals/benchmarks/data/ (not version
controlled); this package converts it into versioned case files under
evals/benchmarks/samples/, replays them through the real MMS write pipeline
session-by-session and evaluates retrieval-grounded answering with DeepEval.
"""
