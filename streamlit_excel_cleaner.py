# streamlit_excel_cleaner.py
import os
import pandas as pd
import streamlit as st
from io import BytesIO

# List of columns to hide (delete)
columns_to_hide = [
    "Ride ID", "Pickup Time (UTC)", "Pickup Timezone offset from UTC", "Pickup Date (UTC)",
    "Drop-off Time (Local)", "Drop-off Time (UTC)", "Drop-off Timezone", "Drop-off Date (Local)", "Drop-off Date (UTC)", "Email",
    "Pickup City", "Pickup State", "Pickup Zip Code", "Requester Name",
    "Drop-off City", "Drop-off State", "Drop-off Zip Code",
    "Request Address", "Request City", "Request State", "Request Zip Code",
    "Destination Address", "Destination City", "Destination State", "Destination Zip Code",
    "Duration (minutes)", "Ride Fare", "Ride Fees", "Ride Discounts", "Ride Tip", "Ride Cost",
    "Business Services Fee", "Transaction Date (UTC)", "Transaction Time (UTC)", "Transaction Currency", "Transaction Outcome",
    "Expense Code", "Expense Note", "Ride Type", "Employee ID", "Custom Tag 1", "Custom Tag 2",
    "Fare Type", "Scheduled Ride Id", "Flex Ride Id", "Flex Ride", "Pickup Latitude", "Pickup Longitude",
    "Drop-off Latitude", "Drop-off Longitude",
    "Trip/Eats ID", "Transaction Timestamp (UTC)", "Request Date (UTC)", "Request Time (UTC)", "Request Date (Local)", "Request Time (Local)",
    "Request Type", "Request Timezone Offset from UTC", "Service", "City", "Haversine Distance (mi)", "Duration (min)", "Drop Off Latitude",
    "Drop Off Longitude", "Expense Code", "Invoices", "Program", "Group", "Payment Method", "Fare in Local Currency (excl. Taxes)", "Taxes in Local Currency",
    "Tip in Local Currency", "Taxes in Local Currency", "Tip in Local Currency",
    "Local Currency Code", "Fare in USD (excl. Taxes)", "Taxes in USD", "Tip in USD", "Transaction Amount in USD (incl. Taxes)", "Estimated Service and Technology Fee (incl. Taxes, if any) in USD",
    "Health Dashboard URL", "Invoice Number", "Driver First Name", "Deductions in Local Currency", "Member ID", "Plan ID", "Network Transaction Id",
    "IsGroupOrder", "Fulfilment Type", "Country", "Cancellation type", "Membership Savings(Local Currency)", "Granular Service Purpose Type"
]

internal_note_values = ["FCC", "FCM", "FCSH", "FCSC", "DTF"]


def clean_and_group(df):
    df_sorted = df.sort_values(by=['Last Name', 'First Name', 'Passenger Number'],
                               key=lambda col: col.str.lower() if col.dtype == 'object' else col).reset_index(drop=True)

    all_rows = []
    group_rows = []
    current_key = None

    for i in range(len(df_sorted)):
        row = df_sorted.iloc[i]
        group_key = (row['Passenger Number'], row['Last Name'], row['First Name'])

        is_last_row = (i == len(df_sorted) - 1)

        if current_key is None:
            current_key = group_key

        if group_key != current_key:
            group_df = pd.DataFrame(group_rows)
            group_df["Trips Count"] = 1
            group_df["Trips Count"] = group_df["Trips Count"].astype(int)
            all_rows.extend(group_df.to_dict(orient="records"))

            transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
            total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

            totals_row = {col: "" for col in group_df.columns}
            totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
            totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
            all_rows.append(totals_row)
            all_rows.append({col: "" for col in group_df.columns})

            group_rows = []

        group_rows.append(row)
        current_key = group_key

        if is_last_row:
            group_df = pd.DataFrame(group_rows)
            group_df["Trips Count"] = 1
            group_df["Trips Count"] = group_df["Trips Count"].astype(int)
            all_rows.extend(group_df.to_dict(orient="records"))

            transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
            total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

            totals_row = {col: "" for col in group_df.columns}
            totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
            totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
            all_rows.append(totals_row)
            all_rows.append({col: "" for col in group_df.columns})

    return pd.DataFrame(all_rows)


def clean_file(uploaded_file):
    try:
        print("🚀 File received:", uploaded_file)

        is_common_courtesy = False

        if uploaded_file.name.endswith(".csv"):
            preview = pd.read_csv(uploaded_file, nrows=1, header=None)
            uploaded_file.seek(0)  # rewind after preview read
            if "Common Courtesy" in str(preview.iloc[0, 1]):
                df = pd.read_csv(uploaded_file, header=4)
                is_common_courtesy = True
                if 'Guest First Name' in df.columns:
                    df['First Name'] = df['Guest First Name']
                if 'Guest Last Name' in df.columns:
                    df['Last Name'] = df['Guest Last Name']
                df.drop(columns=['Guest First Name', 'Guest Last Name'], inplace=True, errors='ignore')
            else:
                df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        print("📋 Columns in uploaded file:", df.columns.tolist())

        note_column = next((col for col in ['Internal Note', 'Expense Memo'] if col in df.columns), None)
        print("🧩 Note column used:", note_column)
        print("🔍 Unique note values:", df[note_column].unique())
        required_cols = ['First Name', 'Last Name']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if note_column is None:
            missing_cols.append('Internal Note or Expense Memo')
        if missing_cols:
            return None

        df_filtered = df[df[note_column].isin(internal_note_values)]
        custom_columns_to_hide = columns_to_hide.copy()
        if is_common_courtesy and "Email" in custom_columns_to_hide:
            custom_columns_to_hide.remove("Email")
        df_filtered = df_filtered.drop(columns=[col for col in custom_columns_to_hide if col in df_filtered.columns])
        if is_common_courtesy and "Transaction Type" in df_filtered.columns:
            df_filtered = df_filtered.drop(columns=["Transaction Type"])

        column_rename_map = {
            "Distance (mi)": "Distance (miles)",
            "Transaction Amount in Local Currency (incl. Taxes)": "Transaction Amount",
            "Ride Status": "Transaction Type",
            "Guest Phone Number": "Passenger Number",  
            "Expense Memo": "Internal Note",  
            "Email": 'Email Info',
            "Requester Email": 'Email Info',          
        }
        df_filtered.rename(columns=column_rename_map, inplace=True)

        df_filtered['First Name'] = df_filtered['First Name'].astype(str).str.strip()
        df_filtered['Last Name'] = df_filtered['Last Name'].astype(str).str.strip()
        df_filtered['Passenger Number'] = df_filtered['Passenger Number'].astype(str).str.strip()

        df_filtered_sorted = df_filtered.sort_values(
            by=['Last Name', 'First Name', 'Passenger Number'],
            key=lambda col: col.str.lower() if col.dtype == 'object' else col
        )

        all_rows = []
        df_values = df_filtered_sorted.reset_index(drop=True)
        group_rows = []
        current_key = None

        for i in range(len(df_values)):
            row = df_values.iloc[i]
            group_key = (row['Passenger Number'], row['Last Name'], row['First Name'])

            is_last_row = (i == len(df_values) - 1)
            next_key = (
                (df_values.iloc[i + 1]['Passenger Number'], df_values.iloc[i + 1]['Last Name'], df_values.iloc[i + 1]['First Name'])
                if not is_last_row else None
            )

            if current_key is None:
                current_key = group_key

            if group_key != current_key:
                group_df = pd.DataFrame(group_rows)
                group_df["Trips Count"] = 1
                group_df["Trips Count"] = group_df["Trips Count"].astype(int)
                all_rows.extend(group_df.to_dict(orient="records"))

                transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
                total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

                totals_row = {col: "" for col in group_df.columns}
                totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
                totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
                all_rows.append(totals_row)
                all_rows.append({col: "" for col in group_df.columns})

                group_rows = []

            group_rows.append(row)
            current_key = group_key

            if is_last_row:
                group_df = pd.DataFrame(group_rows)
                group_df["Trips Count"] = 1
                group_df["Trips Count"] = group_df["Trips Count"].astype(int)
                all_rows.extend(group_df.to_dict(orient="records"))

                transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
                total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

                totals_row = {col: "" for col in group_df.columns}
                totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
                totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
                all_rows.append(totals_row)
                all_rows.append({col: "" for col in group_df.columns})

        final_df = pd.DataFrame(all_rows)

        print("✅ Finished cleaning file, returning DataFrame")

        return final_df

    except Exception as e:
        print("Error:", e)
        return None


# --- Streamlit UI ---
st.set_page_config(page_title="Monthly Report Tool", layout="centered")
st.title("📊 Monthly Report Tool")
st.markdown("Upload your Excel or CSV file to clean and summarize your data.")

uploaded_file = st.file_uploader("Upload .xlsx or .csv file", type=["xlsx", "csv"])

if uploaded_file:
    cleaned_df = clean_file(uploaded_file)
    print("🧪 Shape after filtering:", cleaned_df)

    if cleaned_df is not None:
        st.success("✅ File cleaned successfully!")
        st.dataframe(cleaned_df.head(50))

        output = BytesIO()
        cleaned_df.to_excel(output, index=False)
        output.seek(0)
        st.download_button("📥 Download Cleaned File", output, file_name="cleaned_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# --- Visual Divider ---
st.markdown("""
<hr style="border: none; border-top: 2px dashed #bbb; margin: 40px 0;">
""", unsafe_allow_html=True)

# --- Combine Two Files Section ---
st.markdown("#### 📎 Combine Two Filtered Files & Split by Internal Notes")

uploaded_file1 = st.file_uploader("Upload your first .xlsx or .csv file", type=["xlsx", "csv"], key="file1")
uploaded_file2 = st.file_uploader("Upload your second .xlsx or .csv file", type=["xlsx", "csv"], key="file2")

def sort_and_merge(file1_path, file2_path):
    import pandas as pd

    internal_note_values = ["FCC", "FCM", "FCSH", "FCSC", "DTF"]

    def clean_and_sort(file_obj):
        file_obj.seek(0)
        if file_obj.name.endswith(".csv"):
            preview = pd.read_csv(file_obj, nrows=1, header=None)
            file_obj.seek(0)  # 🔁 Must reset again before full read
            if "Common Courtesy" in str(preview.iloc[0, 1]):
                file_obj.seek(0)
                df = pd.read_csv(file_obj, header=4)
            else:
                file_obj.seek(0)
                df = pd.read_csv(file_obj)
        else:
            file_obj.seek(0)
            df = pd.read_excel(file_obj)

        df_filtered = df[df['Internal Note'].isin(internal_note_values)]
        df_filtered['Last Name'] = df_filtered['Last Name'].astype(str).str.strip()
        df_filtered['First Name'] = df_filtered['First Name'].astype(str).str.strip()
        df_filtered['Passenger Number'] = df_filtered['Passenger Number'].astype(str).str.strip()

        return df_filtered.sort_values(
            by=['Last Name', 'First Name', 'Passenger Number'],
            key=lambda col: col.str.lower() if col.dtype == 'object' else col
        )


    df1 = clean_and_sort(file1_path)
    df2 = clean_and_sort(file2_path)

    combined_df = pd.concat([df1, df2], ignore_index=True)
    df_sorted = combined_df.sort_values(
        by=['Last Name', 'First Name', 'Passenger Number'],
        key=lambda col: col.str.lower() if col.dtype == 'object' else col
    ).reset_index(drop=True)

    all_rows = []
    group_rows = []
    current_key = None

    for i in range(len(df_sorted)):
        row = df_sorted.iloc[i]
        group_key = (row['Passenger Number'], row['Last Name'], row['First Name'])

        is_last_row = (i == len(df_sorted) - 1)
        next_key = (
            (df_sorted.iloc[i + 1]['Passenger Number'], df_sorted.iloc[i + 1]['Last Name'], df_sorted.iloc[i + 1]['First Name'])
            if not is_last_row else None
        )

        if current_key is None:
            current_key = group_key

        if group_key != current_key:
            group_df = pd.DataFrame(group_rows)
            group_df["Trips Count"] = 1
            group_df["Trips Count"] = group_df["Trips Count"].astype(int)
            all_rows.extend(group_df.to_dict(orient="records"))

            transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
            total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

            totals_row = {col: "" for col in group_df.columns}
            totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
            totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
            all_rows.append(totals_row)
            all_rows.append({col: "" for col in group_df.columns})  # Empty row

            group_rows = []

        group_rows.append(row)
        current_key = group_key

        if is_last_row:
            group_df = pd.DataFrame(group_rows)
            group_df["Trips Count"] = 1
            group_df["Trips Count"] = group_df["Trips Count"].astype(int)
            all_rows.extend(group_df.to_dict(orient="records"))

            transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
            total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

            totals_row = {col: "" for col in group_df.columns}
            totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
            totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
            all_rows.append(totals_row)
            all_rows.append({col: "" for col in group_df.columns})

    final_df = pd.DataFrame(all_rows)
    return final_df

def split_by_internal_note(df):
    split_files = {}

    if 'Internal Note' not in df.columns:
        return {}

    for note in internal_note_values:
        df_note = df[df['Internal Note'] == note].copy()
        if df_note.empty:
            continue

        df_note['Last Name'] = df_note['Last Name'].astype(str).str.strip()
        df_note['First Name'] = df_note['First Name'].astype(str).str.strip()
        df_note['Passenger Number'] = df_note['Passenger Number'].astype(str).str.strip()

        df_note = df_note.sort_values(
            by=['Last Name', 'First Name', 'Passenger Number'],
            key=lambda col: col.str.lower() if col.dtype == 'object' else col
        ).reset_index(drop=True)

        all_rows = []
        group_rows = []
        current_key = None

        for i in range(len(df_note)):
            row = df_note.iloc[i]
            group_key = (row['Passenger Number'], row['Last Name'], row['First Name'])

            is_last_row = (i == len(df_note) - 1)

            if current_key is None:
                current_key = group_key

            if group_key != current_key:
                group_df = pd.DataFrame(group_rows)
                group_df["Trips Count"] = 1
                group_df["Trips Count"] = group_df["Trips Count"].astype(int)
                all_rows.extend(group_df.to_dict(orient="records"))

                transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
                total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

                totals_row = {col: "" for col in group_df.columns}
                totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
                totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
                all_rows.append(totals_row)
                all_rows.append({col: "" for col in group_df.columns})

                group_rows = []

            group_rows.append(row)
            current_key = group_key

            if is_last_row:
                group_df = pd.DataFrame(group_rows)
                group_df["Trips Count"] = 1
                group_df["Trips Count"] = group_df["Trips Count"].astype(int)
                all_rows.extend(group_df.to_dict(orient="records"))

                transaction_col = next((col for col in ["Transaction Amount", "Transaction Amount in Local Currency (incl. Taxes)"] if col in group_df.columns), None)
                total_transaction = pd.to_numeric(group_df[transaction_col], errors="coerce").sum() if transaction_col else 0

                totals_row = {col: "" for col in group_df.columns}
                totals_row[transaction_col or "Transaction Amount"] = round(total_transaction, 2)
                totals_row["Trips Count"] = int(group_df["Trips Count"].sum())
                all_rows.append(totals_row)
                all_rows.append({col: "" for col in group_df.columns})

        final_df = pd.DataFrame(all_rows)
        output = BytesIO()
        final_df.to_excel(output, index=False)
        output.seek(0)
        split_files[note] = output

    return split_files

if uploaded_file1 and uploaded_file2:
    try:
        st.info("🟡 Merging files...")
        df = sort_and_merge(uploaded_file1, uploaded_file2)
        st.success("✅ Files merged successfully!")
        st.dataframe(df.head(50))  # Show preview

        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        st.download_button(
            "📥 Download Merged File",
            output,
            file_name="merged_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Split by Internal Note
        st.markdown("### 🔍 Download Split Files by Internal Note")

        split_files = split_by_internal_note(df)
        if not split_files:
            st.warning("⚠️ Could not find 'Internal Note' column to split by.")
        else:
            for note, file_io in split_files.items():
                st.download_button(
                    label=f"📂 Download {note}.xlsx",
                    data=file_io,
                    file_name=f"{note}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    except Exception as e:
        st.error(f"❌ Merge failed: {e}")
