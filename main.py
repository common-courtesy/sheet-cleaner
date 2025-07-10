from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.responses import JSONResponse

from typing import List
from io import BytesIO

from streamlit_excel_cleaner import clean_file
from streamlit_excel_cleaner import sort_and_merge
from streamlit_excel_cleaner import split_by_internal_note

import zipfile
import tempfile
import base64

from pandas import read_excel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # or ["*"] for all origins during testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

    try:
        df = read_excel(uploaded_file)
    except Exception as e:
        return {"error": f"Failed to read Excel file: {str(e)}"}

    split_files = split_by_internal_note(df)
    if not split_files:
        return {"error": "Could not split. 'Internal Note' missing or empty."}

    response_data = {}
    for note, file_io in split_files.items():
        file_io.seek(0)
        b64_file = base64.b64encode(file_io.read()).decode('utf-8')
        response_data[note] = b64_file

    return JSONResponse(content=response_data)
