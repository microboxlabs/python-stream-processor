"""Tests for HLSGenerator.generate_segments_batch (FFmpeg mocked — CI-safe)."""

import subprocess
from pathlib import Path

from stream_processor.service.hls_generator import HLSGenerator
from stream_processor.service.storage_backend import create_storage_backend


def _gen(tmp_path):
    fs = create_storage_backend("filesystem", str(tmp_path / "store"), None, None)
    return HLSGenerator(storage=fs)


def test_parse_extinf(tmp_path):
    pl = tmp_path / "x.m3u8"
    pl.write_text("#EXTM3U\n#EXTINF:6.000000,\nseg_000000.ts\n#EXTINF:5.500,\nseg_000001.ts\n")
    assert HLSGenerator._parse_extinf(str(pl)) == [6.0, 5.5]


def _fake_ffmpeg(cmd, **kwargs):
    """Emulate the HLS muxer: create seg files + a playlist from the cmd flags."""
    seg_pattern = cmd[cmd.index("-hls_segment_filename") + 1]
    base = int(cmd[cmd.index("-start_number") + 1])
    total = float(cmd[cmd.index("-t") + 1])
    hls_time = float(cmd[cmd.index("-hls_time") + 1])
    n = int(round(total / hls_time))
    out_dir = Path(seg_pattern).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    extinf = []
    for i in range(n):
        (out_dir / f"seg_{base + i:06d}.ts").write_bytes(b"ts")
        extinf.append(f"#EXTINF:{hls_time:.6f},\nseg_{base + i:06d}.ts")
    Path(cmd[-1]).write_text("#EXTM3U\n" + "\n".join(extinf) + "\n")
    return subprocess.CompletedProcess(cmd, 0, "", "")


class TestBatchEncoder:
    def test_truncates_to_full_segments_and_numbers(self, tmp_path, monkeypatch):
        gen = _gen(tmp_path)
        fps = gen.config.frames_per_segment
        # 2 full segments worth + 2 leftover frames.
        frames = []
        for i in range(2 * fps + 2):
            f = tmp_path / f"f{i}.jpg"
            f.write_bytes(b"x")
            frames.append(str(f))

        monkeypatch.setattr("stream_processor.service.hls_generator.subprocess.run", _fake_ffmpeg)
        results = gen.generate_segments_batch("c", "d", frames, base_segment_number=100)

        # Only complete segments, numbered contiguously from base.
        assert [num for num, _ in results] == [100, 101]
        # Durations come from the muxer playlist (== segment_duration).
        assert all(dur == gen.config.segment_duration_seconds for _, dur in results)

    def test_skips_when_fewer_than_one_segment(self, tmp_path):
        gen = _gen(tmp_path)
        f = tmp_path / "only.jpg"
        f.write_bytes(b"x")
        # 1 frame < frames_per_segment -> nothing to encode, no ffmpeg call.
        assert gen.generate_segments_batch("c", "d", [str(f)], base_segment_number=0) == []

    def test_command_uses_hls_muxer_and_start_number(self, tmp_path, monkeypatch):
        gen = _gen(tmp_path)
        fps = gen.config.frames_per_segment
        frames = []
        for i in range(fps):
            f = tmp_path / f"f{i}.jpg"
            f.write_bytes(b"x")
            frames.append(str(f))

        captured = {}

        def capture(cmd, **kwargs):
            captured["cmd"] = cmd
            return _fake_ffmpeg(cmd, **kwargs)

        monkeypatch.setattr("stream_processor.service.hls_generator.subprocess.run", capture)
        gen.generate_segments_batch("c", "d", frames, base_segment_number=42)

        cmd = captured["cmd"]
        assert "-f" in cmd and "hls" in cmd
        assert cmd[cmd.index("-start_number") + 1] == "42"
        assert cmd[cmd.index("-hls_time") + 1] == str(gen.config.segment_duration_seconds)
        # duration cap == one segment here
        assert cmd[cmd.index("-t") + 1] == str(gen.config.segment_duration_seconds)
