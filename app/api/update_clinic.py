from fastapi import APIRouter, Request, HTTPException, status, Header
import pandas as pd
from datetime import datetime
import pytz
from typing import Optional
from ..services.utils import fetchFromDB
from ..services.base_service import BaseService

router = APIRouter()

@router.post("/update_clinic")
async def update_clinic(
    request: Request,
    x_update_secret: Optional[str] = Header(None)
):
    """Update clinic email list from database"""

    if not x_update_secret:
        raise HTTPException(
            status_code=403,
            detail="Update secret token required"
        )

    try:
        base_service = BaseService()
        clinic_old = base_service.clinic.copy()
        query = base_service.update_clinic_email_query
        query = str(query)
        clinic_update = fetchFromDB(query)
        
        old_group = clinic_old.sort_values(by=['clinicId', 'clinicEmail', 'clinicName']).groupby(['clinicId', 'clinicName'])
        keyword = pd.DataFrame()

        for _, group in old_group:
            colKeyword, colPC = [], []
            for _, row in group.iterrows():
                if pd.notna(row['keyword']) and row['keyword'] not in colKeyword:
                    if ',' in row['keyword']:
                        keywords = [kw.strip() for kw in row['keyword'].split(',')]
                        colKeyword.extend([kw for kw in keywords if kw])
                    else:
                        colKeyword.append(row['keyword'])
                if pd.notna(row['provetCloud']) and row['provetCloud'] not in colPC:
                    colPC.append(row['provetCloud'])

            i = group.index[0]
            keyword.at[i, 'clinicId'] = group['clinicId'].iloc[0]
            keyword.at[i, 'keyword'] = ', '.join(colKeyword) if colKeyword else None
            keyword.at[i, 'provetCloud'] = ', '.join(colPC) if colPC else None

        clinic_temp = pd.merge(
            clinic_update,
            clinic_old[['clinicId','clinicEmail','keyword','provetCloud']],
            on=['clinicId','clinicEmail'],
            how='left'
        ).drop_duplicates()

        main = pd.merge(
            clinic_temp.loc[clinic_temp['role']=='main_email'],
            keyword,
            on=['clinicId'],
            how='left',
            suffixes=('_old', '')
        )
        main['keyword'] = main['keyword'].fillna(main['keyword_old'])
        main['provetCloud'] = main['provetCloud'].fillna(main['provetCloud_old'])
        main.drop(columns=['keyword_old', 'provetCloud_old'], inplace=True)

        remain = clinic_temp.loc[clinic_temp['role']!='main_email']
        clinic_new = pd.concat([main, remain], ignore_index=True).sort_values(
            by=['clinicId', 'clinicEmail', 'clinicName']
        ).reset_index(drop=True)

        clinic_new.loc[:,'clinicName'] = clinic_new.loc[:,'clinicName'].str.strip()

        # Write back to storage (handle GCS and local differently)
        clinic_csv_path = f"{base_service.folder}/clinic.csv"

        if clinic_csv_path.startswith("gs://"):
            try:
                from google.cloud import storage
                import io

                path_parts = clinic_csv_path.replace("gs://", "").split("/", 1)
                bucket_name = path_parts[0]
                blob_name = path_parts[1]

                csv_buffer = io.StringIO()
                clinic_new.to_csv(csv_buffer, index=False)
                csv_content = csv_buffer.getvalue()

                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(blob_name)
                blob.upload_from_string(csv_content, content_type='text/csv')

            except ImportError:
                raise HTTPException(
                    status_code=500,
                    detail="Google Cloud Storage client not available"
                )
            except Exception as gcs_error:
                print(f"GCS upload error: {str(gcs_error)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to upload to GCS: {str(gcs_error)}"
                )
        else:
            clinic_new.to_csv(clinic_csv_path, index=False)

        BaseService._data_cache['clinic'] = clinic_new.copy()
        base_service.clinic = clinic_new.copy()

        tz = pytz.timezone("Europe/Stockholm")
        update_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        print(f"=== ✅ Clinic email list updated successfully in {update_time} to gcs file: {clinic_csv_path}====")

        return {
            "status": "success",
            "message": "Clinic email list updated successfully",
            "update_time": update_time,
            "records_processed": len(clinic_new)
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update clinic list: {str(e)}"
        )