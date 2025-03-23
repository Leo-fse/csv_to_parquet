import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import polars as pl


def get_new_data(input_dir: Path, use_streaming: bool = True):
    """
    CSVファイルを読み込み、結合・重複除去処理を行い、新規データをArrow Tableとして返す。
    """
    name_pattern = ["USER"]

    csv_lf_list = []
    csv_files = list(Path(input_dir).glob("*.csv"))
    print(f"Processing {len(csv_files)} CSV files...")
    for csv_file in csv_files:
        if any(name in csv_file.name for name in name_pattern):
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
            print(f"Extracting {len(csv_files)} csv files from {zip_file}")
            for zip_info in zip_ref.infolist():
                if zip_info.filename.endswith(".csv"):
                    print(f"Extracting {zip_info.filename}...")
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        # extract() の戻り値で正確なパスを得る
                        extracted_file_path = zip_ref.extract(zip_info, tmp_dir)
                        lf_zip = format_csv_data(Path(extracted_file_path))
                        if lf_zip is not None:
                            zipped_csv_lf_list.append(lf_zip.collect())

    print(
        f"処理完了: {len(csv_lf_list)} CSV files, {len(zipped_csv_lf_list)} ZIP files"
    )

    zipped_csv_lf = pl.concat(zipped_csv_lf_list).lazy()
    csv_lf = pl.concat(csv_lf_list)
    all_lf = pl.concat([csv_lf, zipped_csv_lf])
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
    lf = lf.sort(by=["Time", "Item", "created_at"])
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

    return lf_deduped.sort("Time", "Item")


def format_csv_data(fp):
    encoding = "utf8"
    print(f"Reading {fp}...")

    # ヘッダー情報の取得
    headers = []
    # ヘッダー行の読み込み（エンコーディングを試行）
    try:
        with open(fp, "r", encoding=encoding) as f:
            headers1 = f.readline().strip().split(",")
            headers2 = f.readline().strip().split(",")
            headers3 = f.readline().strip().split(",")
    except UnicodeDecodeError:
        # shift-jisを試す
        encoding = "shift-jis"

        try:
            with open(fp, "r", encoding=encoding) as f:
                headers1 = f.readline().strip().split(",")
                headers2 = f.readline().strip().split(",")
                headers3 = f.readline().strip().split(",")
        except UnicodeDecodeError:
            # cp932を試す
            encoding = "cp932"
            try:
                with open(fp, "r", encoding=encoding) as f:
                    headers1 = f.readline().strip().split(",")
                    headers2 = f.readline().strip().split(",")
                    headers3 = f.readline().strip().split(",")
            except UnicodeDecodeError:
                print(f"エンコーディングの判定に失敗しました: {fp}")
                return None

    # 1列目は日時カラムなので、ヘッダーを"Time"に変更
    headers1[0] = ""
    headers2[0] = "Time"
    headers3[0] = ""

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
