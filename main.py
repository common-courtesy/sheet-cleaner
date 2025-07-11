from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse, FileResponse, StreamingResponse

from typing import List
from io import BytesIO

from streamlit_excel_cleaner import clean_file
from streamlit_excel_cleaner import sort_and_merge
from streamlit_excel_cleaner import split_by_internal_note

import zipfile
import uuid
import zipfile
import base64
import os

from pandas import read_excel
import pandas as pd

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # or ["*"] for all origins during testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

@app.post("/clean")
async def clean_uploaded_file(file: UploadFile = File(...)):
    contents = await file.read()
    uploaded_file = BytesIO(contents)
    
    # ✅ Add the expected attributes
    uploaded_file.name = file.filename
    uploaded_file.type = file.content_type
    uploaded_file.size = len(contents)

    result = clean_file(uploaded_file)
    if result is None:
        return {"error": "Cleaning failed or required columns missing."}

    cleaned_df, output = result
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": "attachment; filename=cleaned_report.xlsx"
    })

@app.post("/merge")
async def merge_two_files(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    contents1 = await file1.read()
    contents2 = await file2.read()

    file1_obj = BytesIO(contents1)
    file1_obj.name = file1.filename

    file2_obj = BytesIO(contents2)
    file2_obj.name = file2.filename

    try:
        df, output = sort_and_merge(file1_obj, file2_obj)
    except Exception as e:
        return {"error": str(e)}

    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": "attachment; filename=merged_report.xlsx"
    })

@app.post("/split")
async def split_file_by_internal_note(file: UploadFile = File(...)):
    contents = await file.read()
    uploaded_file = BytesIO(contents)
    uploaded_file.name = file.filename

    print("✅ Merge result shape:", df.shape)
    print(df.head())

    print(f"Received file: {file.filename}, size: {len(contents)} bytes")

    try:
        print("Trying to read file with openpyxl...")
        df = read_excel(uploaded_file, engine='openpyxl')
    except Exception as e1:
        try:
            uploaded_file.seek(0)
            df = read_excel(uploaded_file, engine='xlrd')
        except Exception as e2:
            return {"error": f"Failed to read Excel file. openpyxl: {e1}, xlrd: {e2}"}

    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")

    print("Splitting DataFrame by internal notes...")
    split_files = split_by_internal_note(df)
    if not split_files:
        return {"error": "Could not split. 'Internal Note' missing or empty."}

    preview_data = {}
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for note, file_io in split_files.items():
            file_io.seek(0)
            temp_df = pd.read_excel(file_io)
            preview_data[note] = temp_df.head(10).fillna("").to_dict(orient="records")

            # Rewind again before writing to zip
            file_io.seek(0)
            zf.writestr(f"{note}.xlsx", file_io.read())

    zip_buffer.seek(0)

    zip_data = zip_buffer.read()

    # Store zip in memory temporarily
    zip_b64 = base64.b64encode(zip_buffer.read()).decode("utf-8")

    # Save ZIP to disk for debugging
    debug_zip_path = os.path.join(DOWNLOAD_DIR, "debug_split.zip")
    try:
        with open(debug_zip_path, "wb") as f:
            f.write(base64.b64decode(zip_b64))
        print("✅ Wrote debug_split.zip to:", debug_zip_path)
        print("📏 Zip size (bytes):", os.path.getsize(debug_zip_path))
    except Exception as e:
        print("❌ Failed to write debug zip:", str(e))

    # Clean NaN values from preview
    for key in preview_data:
        for row in preview_data[key]:
            for k, v in row.items():
                if pd.isna(v):
                    row[k] = None

    return JSONResponse(content={
        "preview": preview_data,
        "zip_base64": zip_b64  # Let frontend decode and download if needed
    })

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    return {"error": "File not found"}
