#!/bin/bash
#
# Test FFmpeg segment generation for HLS
# Verifies that FPS and duration match expected values
#

set -e

# Configuration (matches python-stream-processor defaults)
NUM_FRAMES=${1:-6}
FRAME_DURATION=${2:-5}
OUTPUT_FRAMERATE=${3:-1}
VIDEO_WIDTH=${4:-1920}

# Calculated values
EXPECTED_DURATION=$((NUM_FRAMES * FRAME_DURATION))
EXPECTED_FRAMES=$((EXPECTED_DURATION * OUTPUT_FRAMERATE))

# Setup
WORK_DIR=$(mktemp -d)
trap "rm -rf $WORK_DIR" EXIT

echo "=== FFmpeg Segment Generation Test ==="
echo ""
echo "Configuration:"
echo "  Frames:           $NUM_FRAMES"
echo "  Frame duration:   ${FRAME_DURATION}s"
echo "  Output framerate: ${OUTPUT_FRAMERATE} fps"
echo "  Video width:      ${VIDEO_WIDTH}px"
echo ""
echo "Expected output:"
echo "  Duration:         ${EXPECTED_DURATION}s"
echo "  Total frames:     $EXPECTED_FRAMES"
echo ""

# Generate test frames with different colors
echo "Generating $NUM_FRAMES test frames..."
COLORS=("red" "green" "blue" "yellow" "cyan" "magenta" "orange" "purple")

for i in $(seq 1 $NUM_FRAMES); do
    COLOR_IDX=$(( (i - 1) % ${#COLORS[@]} ))
    COLOR=${COLORS[$COLOR_IDX]}
    ffmpeg -hide_banner -loglevel error \
        -f lavfi -i "color=c=${COLOR}:s=${VIDEO_WIDTH}x1080:d=1" \
        -frames:v 1 \
        -vf "drawtext=text='Frame $i':fontsize=72:fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2" \
        "$WORK_DIR/frame_$(printf '%03d' $i).jpg"
done

# Create concat demuxer input file
echo "Creating FFmpeg input list..."
INPUT_LIST="$WORK_DIR/input.txt"

for i in $(seq 1 $NUM_FRAMES); do
    echo "file '$WORK_DIR/frame_$(printf '%03d' $i).jpg'" >> "$INPUT_LIST"
    echo "duration $FRAME_DURATION" >> "$INPUT_LIST"
done

# Repeat last frame (required by concat demuxer)
echo "file '$WORK_DIR/frame_$(printf '%03d' $NUM_FRAMES).jpg'" >> "$INPUT_LIST"

# Run FFmpeg (matches hls_generator.py:226-258)
echo "Running FFmpeg..."
OUTPUT_FILE="$WORK_DIR/test_segment.ts"

ffmpeg -hide_banner -loglevel warning \
    -y \
    -f concat -safe 0 -i "$INPUT_LIST" \
    -c:v libx264 -preset ultrafast -tune zerolatency \
    -profile:v baseline -level 4.0 \
    -vf "scale=${VIDEO_WIDTH}:-2" -pix_fmt yuv420p \
    -r "$OUTPUT_FRAMERATE" \
    -t "$EXPECTED_DURATION" \
    -f mpegts -mpegts_copyts 1 \
    "$OUTPUT_FILE"

# Verify output
echo ""
echo "=== Verification ==="

# Get framerate (take first line only)
ACTUAL_FRAMERATE=$(ffprobe -v error -select_streams v:0 \
    -show_entries stream=r_frame_rate \
    -of default=noprint_wrappers=1:nokey=1 "$OUTPUT_FILE" | head -1)

# Get duration
ACTUAL_DURATION=$(ffprobe -v error -select_streams v:0 \
    -show_entries stream=duration \
    -of default=noprint_wrappers=1:nokey=1 "$OUTPUT_FILE" | head -1)

# Count frames (nb_frames not available for mpegts, use packet count)
ACTUAL_FRAMES=$(ffprobe -v error -select_streams v:0 \
    -count_packets -show_entries stream=nb_read_packets \
    -of default=noprint_wrappers=1:nokey=1 "$OUTPUT_FILE" | head -1)

echo "Framerate:  $ACTUAL_FRAMERATE (expected: ${OUTPUT_FRAMERATE}/1)"
echo "Duration:   ${ACTUAL_DURATION}s (expected: ${EXPECTED_DURATION}s)"
echo "Frames:     $ACTUAL_FRAMES (expected: $EXPECTED_FRAMES)"

# Check results
echo ""
PASS=true

if [[ "$ACTUAL_FRAMERATE" != "${OUTPUT_FRAMERATE}/1" ]]; then
    echo "FAIL: Framerate mismatch"
    PASS=false
fi

# Compare duration using awk for floating point (allow 1s tolerance)
DURATION_OK=$(awk -v actual="$ACTUAL_DURATION" -v expected="$EXPECTED_DURATION" \
    'BEGIN { diff = actual - expected; if (diff < 0) diff = -diff; print (diff <= 1) ? "1" : "0" }')

if [[ "$DURATION_OK" != "1" ]]; then
    echo "FAIL: Duration mismatch (tolerance: 1s)"
    PASS=false
fi

# Allow 1 frame tolerance for MPEG-TS container overhead
FRAME_DIFF=$((ACTUAL_FRAMES - EXPECTED_FRAMES))
if [[ $FRAME_DIFF -lt 0 ]]; then
    FRAME_DIFF=$((-FRAME_DIFF))
fi

if [[ $FRAME_DIFF -gt 1 ]]; then
    echo "FAIL: Frame count mismatch (tolerance: 1 frame)"
    PASS=false
fi

if $PASS; then
    echo "PASS: All checks passed"
    exit 0
else
    echo ""
    echo "Some checks failed. Review the output above."
    exit 1
fi
