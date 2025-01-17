import azure.functions as func
from extract import extract_data
from transform import transform_data
from load import load_data
import json

app = func.FunctionApp()

@app.function_name(name="ExtractFunction")
@app.route(route="extract", auth_level=func.AuthLevel.FUNCTION)
def extract(req: func.HttpRequest) -> func.HttpResponse:
    try:
        blob_name = extract_data()
        return func.HttpResponse(
            body=json.dumps({"message": "Extraction complete", "blob_name": blob_name}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Extraction failed: {str(e)}", status_code=500)


@app.function_name(name="TransformFunction")
@app.route(route="transform", auth_level=func.AuthLevel.FUNCTION)
def transform(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse the request body
        req_body = req.get_json()
        blob_name = req_body.get('blob_name')  # Fetch 'blob_name' from the request body
        #blob_name = 'recently_sold_axies_20250106005826.json'
        if not blob_name:
            return func.HttpResponse("Blob name is required", status_code=400)
        
        file_list = transform_data(blob_name)
        return func.HttpResponse(
            body=json.dumps({"message": f"Transformation complete for blob: {blob_name}", "file_list": file_list}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Transformation failed: {str(e)}", status_code=500)


@app.function_name(name="LoadFunction")
@app.route(route="load", auth_level=func.AuthLevel.FUNCTION)
def load(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse the request body
        req_body = req.get_json()
        file_list = req_body.get('file_list')  # Fetch 'file_list' from the request body

        #file_list = "axies_20250106011357.csv,order_offers_20250106011357.csv"
        if not file_list:
            return func.HttpResponse("File list is required", status_code=400)

        # Call the load_data function with the provided file list
        load_data(file_list)
        return func.HttpResponse("Load complete", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Load failed: {str(e)}", status_code=500)
