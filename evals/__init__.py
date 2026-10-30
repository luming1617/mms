"""Offline model-quality evaluation module for the Memory Management Service.

Implements docs/1-模型质量评测体系设计.md: versioned golden datasets,
deterministic contract metrics, optional LLM-as-a-judge metrics, a harness that
drives the real FastAPI application, per-case result persistence and CI gate
integration via scripts/quality_gate.py.
"""
