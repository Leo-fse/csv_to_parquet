import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import polars as pl

from libs.settings import ENCODING_OPTIONS, NAME_PATTERNS


def get_new_data(input_dir: Path, use_streaming: bool = True):
    """
    CSVファイルを読み込み、結合・重複除去処理を行い、新規データをArrow Tableとして返す。
    """

    csv_lf_list = []
    csv_files = [
        csv
        for csv in list(Path(input_dir).glob("*.csv"))
        if any(name in csv.name for name in NAME_PATTERNS)
    ]
    print(f"Processing {len(csv_files)} CSV files...")
    for csv_file in csv_files:
        print(f"処理中: {csv_file}")
        lf = format_csv_data(csv_file)
        if lf is not None:
            csv_lf_list.append(lf)
    csv_lf = pl.concat(csv_lf_list)

    zip_files = list(Path(input_dir).glob("*.zip"))
    print(f"Processing {len(zip_files)} ZIP files...")

    zipped_csv_lf_list = []
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            csv_files = [name for name in zip_ref.namelist() if name.endswith(".csv")]
            print(f"Processing {len(csv_files)} csv files from {zip_file}")
            for zip_info in zip_ref.infolist():
                if zip_info.filename.endswith(".csv") and any(
                    name in zip_info.filename for name in NAME_PATTERNS
                ):
                    print(f"処理中： {zip_info.filename}...")
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        # extract() の戻り値で正確なパスを得る
                        extracted_file_path = zip_ref.extract(zip_info, tmp_dir)
                        lf_zip = format_csv_data(Path(extracted_file_path))
                        if lf_zip is not None:
                            zipped_csv_lf_list.append(
                                lf_zip.collect(engine="streaming")
                            )

    print(
        f"{len(csv_lf_list)} CSV files, {len(zipped_csv_lf_list)} ZIP files の処理を実行しました"
    )

    csv_lf = pl.concat(csv_lf_list)

    if zipped_csv_lf_list:
        zipped_csv_lf = pl.concat(zipped_csv_lf_list).lazy()
        all_lf = pl.concat([csv_lf, zipped_csv_lf])
    else:
        all_lf = csv_lf

    lf_deduped = process_df(all_lf)

    if use_streaming:
        df = lf_deduped.collect(engine="streaming")
    else:
        df = lf_deduped.collect()

    # # Arrow Table に変換して返す
    return df.to_arrow()


def process_df(lf, is_overwrite=True):
    original_names = lf.collect_schema().names()
    new_names = original_names.copy()
    new_names[0] = "Time"
    lf = lf.rename(dict(zip(original_names, new_names)))

    # 重複しているデータを削除する
    # is_overwrite = True : created_atの新しい方を残す
    # is_overwrite = False: created_atの古い方を残す
    if is_overwrite:
        lf_deduped = lf.unique(subset=["Time", "Item"], keep="first")
    else:
        lf_deduped = lf.unique(subset=["Time", "Item"], keep="last")

    lf_deduped = lf_deduped.with_columns(
        pl.col("Time").str.strptime(pl.Datetime, "%Y/%m/%d %H:%M:%S"),
    )
    lf_deduped = lf_deduped.with_columns(
        pl.col("Time").dt.year().alias("year"),
        pl.col("Time").dt.month().alias("month"),
    )

    return lf_deduped.sort(by=["Time", "Item", "created_at"])


def format_csv_data(fp):
    # ヘッダー行の読み込み（エンコーディングを試行）
    for encoding in ENCODING_OPTIONS:
        try:
            with open(fp, "r", encoding=encoding) as f:
                headers1 = f.readline().strip().split(",")
                headers2 = f.readline().strip().split(",")
                headers3 = f.readline().strip().split(",")
            break
        except UnicodeDecodeError:
            continue
    else:  # 全てのエンコーディングでエラーが発生した場合
        print(f"エンコーディングの判定に失敗しました: {fp}")
        return None

    # 1列目は日時カラムなので、ヘッダーを"Time"に変更
    headers1[0], headers2[0], headers3[0] = "", "Time", ""

    # 重複のないヘッダーを作成
    headers = create_unique_headers(headers1, headers2, headers3)

    # データの取得
    lf = pl.scan_csv(fp, has_header=False, skip_rows=3, encoding=encoding)
    # 最終列は不要なので削除
    lf = lf.select(pl.all().exclude(lf.collect_schema().names()[-1]))
    # ヘッダーを設定
    lf = lf.rename(dict(zip(lf.collect_schema().names(), headers)))

    # # lfを縦持ちデータに変換する
    lf = lf.unpivot(index=["<|>Time<|>"], variable_name="Item", value_name="Value")

    # source_file/created_at
    lf = lf.with_columns(
        pl.lit(fp.name).alias("source_file"),
        pl.lit(datetime.now().isoformat()).alias("created_at"),
    )

    return lf


# 重複を防ぐために連番を追加する関数
def create_unique_headers(headers1, headers2, headers3):
    combined_headers = []
    seen_headers = {}

    for h1, h2, h3 in zip(headers1, headers2, headers3):
        combined = f"{h1}<|>{h2}<|>{h3}"

        if combined in seen_headers:
            seen_headers[combined] += 1
            combined = f"{combined}_{seen_headers[combined]}"
        else:
            seen_headers[combined] = 0

        combined_headers.append(combined)

    return combined_headers
