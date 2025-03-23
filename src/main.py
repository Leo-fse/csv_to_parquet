#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from pathlib import Path

from libs.process_csv import get_new_data
from libs.process_parquet import merge_with_existing_data, output_parquet
from libs.settings import INPUT_DIR, OUTPUT_DIR

# 設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_data(input_dir: Path, output_dir: str, use_streaming: bool = True):
    # 新規データの取得
    new_data = get_new_data(input_dir, use_streaming)
    # 処理開始：既存データとのマージ"
    merged_data = merge_with_existing_data(new_data, output_dir)
    # Parquet出力
    output_parquet(merged_data, output_dir)


def main():
    parser = argparse.ArgumentParser(
        description="CSVファイルを前処理してParquet出力するスクリプト"
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        default=str(INPUT_DIR),
        help="入力CSVファイルが格納されているディレクトリのパス",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="出力Parquetファイルのベースディレクトリ",
    )
    parser.add_argument(
        "--no-streaming",
        action="store_true",
        help="streamingモードを無効にする（メモリに全展開）",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = args.output_dir
    use_streaming = not args.no_streaming

    process_data(input_dir, output_dir, use_streaming)


if __name__ == "__main__":
    main()
