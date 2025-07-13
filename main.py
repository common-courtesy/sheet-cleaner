from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse, FileResponse, StreamingResponse
from fastapi.requests import Request

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
    allow_origins=["*"],  
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
    file1_obj.type = file1.content_type
    file1_obj.size = len(contents1)

    file2_obj = BytesIO(contents2)
    file2_obj.name = file2.filename
    file2_obj.type = file2.content_type
    file2_obj.size = len(contents2)

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

    print(f"📥 Received file: {file.filename}, size: {len(contents)} bytes")

    try:
        print("📖 Reading Excel file with openpyxl...")
        df = pd.read_excel(uploaded_file, engine='openpyxl')
    except Exception as e:
        return {"error": f"❌ Failed to read Excel file. Reason: {str(e)}"}

    split_files = split_by_internal_note(df)

    print("🔍 Verifying structure of split_files...")
    if not isinstance(split_files, dict):
        print("❌ split_by_internal_note did not return a dictionary.")
        return {"error": "split_by_internal_note did not return a dictionary."}

    for note, value in split_files.items():
        if not isinstance(value, tuple) or len(value) != 2:
            print(f"❌ Value for '{note}' is not a tuple of length 2: {value}")
            return {"error": f"Value for '{note}' is not a (df, file_io) tuple."}
        
        df_note, file_io = value
        
        if not isinstance(df_note, pd.DataFrame):
            print(f"❌ First item in tuple for '{note}' is not a DataFrame.")
            return {"error": f"First item in tuple for '{note}' is not a DataFrame."}
        
        if not isinstance(file_io, BytesIO):
            print(f"❌ Second item in tuple for '{note}' is not a BytesIO.")
            return {"error": f"Second item in tuple for '{note}' is not a BytesIO."}

    print("✅ split_by_internal_note returned valid (DataFrame, BytesIO) tuples.")

    if not split_files:
        return {"error": "Could not split. 'Internal Note' missing or empty."}

    preview_data = {}
    download_links = {}  # <-- New

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for note, (df_note, file_io) in split_files.items():
            preview_data[note] = df_note.head(10).fillna("").replace({pd.NA: None}).to_dict(orient="records")

            file_io.seek(0)
            b64_excel = base64.b64encode(file_io.read()).decode("utf-8")
            download_links[note] = f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_excel}"

            file_io.seek(0)
            zf.writestr(f"{note}.xlsx", file_io.read())

    zip_buffer.seek(0)
    zip_data = zip_buffer.read()
    zip_b64 = base64.b64encode(zip_data).decode("utf-8")

    # Optional: Save debug zip
    debug_zip_path = os.path.join("downloads", "debug_split.zip")
    try:
        os.makedirs("downloads", exist_ok=True)
        with open(debug_zip_path, "wb") as f:
            f.write(zip_data)
        print("✅ Wrote debug_split.zip to:", debug_zip_path)
        print("📏 Zip size (bytes):", os.path.getsize(debug_zip_path))
    except Exception as e:
        print("❌ Failed to write debug zip:", str(e))

    return JSONResponse(content={
        "preview": preview_data,
        "download_links": download_links,
        "zip_base64": zip_b64  # Optional
    })

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    return {"error": "File not found"}
