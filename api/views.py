from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from .utils import read_any_table, load_dataframe_to_sql, run_sql, TABLE_NAME

def index(request):
    return render(request, "index.html")

@csrf_exempt
def upload_dataset(request):
    if request.method != "POST":
        return HttpResponseBadRequest("Use POST with form-data 'file' (CSV/XLSX).")
    f = request.FILES.get('file') or (next(iter(request.FILES.values()), None))
    if not f:
        return HttpResponseBadRequest("No se recibió archivo. Use clave 'file' en form-data.")
    try:
        df = read_any_table(f)
        load_dataframe_to_sql(df)
        return JsonResponse({"ok": True, "rows": len(df), "columns": list(df.columns)})
    except Exception as e:
        return HttpResponseBadRequest(f"Error loading dataset: {e}")

def schema(request):
    try:
        rows = run_sql(f"PRAGMA table_info({TABLE_NAME})")
        if not rows:
            sample = run_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 0")
            cols = list(sample[0].keys()) if sample else []
            return JsonResponse({"columns": cols})
        cols = [{"name": r.get("name"), "type": r.get("type")} for r in rows]
        return JsonResponse({"columns": cols})
    except Exception as e:
        return HttpResponseBadRequest(f"Error: {e}")

def nulls(request):
    try:
        columns = [c['name'] for c in run_sql(f"PRAGMA table_info({TABLE_NAME})")]
        if not columns:
            sample = run_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 1")
            if sample: columns = list(sample[0].keys())
        if not columns: return JsonResponse({"nulls": []})
        selects = [f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS '{c}'" for c in columns]
        q = f"SELECT {', '.join(selects)} FROM {TABLE_NAME}"
        row = run_sql(q)[0]
        data = [{"column": k, "nulls": v} for k, v in row.items()]
        return JsonResponse({"nulls": data})
    except Exception as e:
        return HttpResponseBadRequest(f"Error: {e}")

def duplicates(request):
    try:
        total = run_sql(f"SELECT COUNT(*) as c FROM {TABLE_NAME}")[0]['c']
        distinct = run_sql(f"SELECT COUNT(*) as c FROM (SELECT DISTINCT * FROM {TABLE_NAME})")[0]['c']
        dups = total - distinct
        return JsonResponse({"total_rows": total, "distinct_rows": distinct, "duplicate_rows": dups})
    except Exception as e:
        return HttpResponseBadRequest(f"Error: {e}")

def categorical_top(request, col):
    try:
        limit = int(request.GET.get("limit", 30))
        q = f"SELECT {col} AS value, COUNT(*) AS count FROM {TABLE_NAME} GROUP BY {col} ORDER BY count DESC LIMIT :limit"
        data = run_sql(q, {"limit": limit})
        return JsonResponse({"series": data})
    except Exception as e:
        return HttpResponseBadRequest(f"Error: {e}")

def numeric_hist(request, col):
    try:
        limit = int(request.GET.get("limit", 100000))
        q = f"SELECT {col} AS value FROM {TABLE_NAME} WHERE {col} IS NOT NULL LIMIT :limit"
        data = run_sql(q, {"limit": limit})
        return JsonResponse({"values": [d['value'] for d in data]})
    except Exception as e:
        return HttpResponseBadRequest(f"Error: {e}")

def datetime_counts(request, col):
    try:
        q = f"SELECT DATE({col}) AS bucket, COUNT(*) AS count FROM {TABLE_NAME} WHERE {col} IS NOT NULL GROUP BY DATE({col}) ORDER BY bucket"
        data = run_sql(q)
        return JsonResponse({"series": data})
    except Exception as e:
        return HttpResponseBadRequest(f"Error: {e}")

from .pandas_desc import describe_current_table
def descripcion_resumen(request):
    try:
        resumen = describe_current_table()
        return JsonResponse({"ok": True, "resumen": resumen})
    except Exception as e:
        return HttpResponseBadRequest(f"Error en descripción (pandas): {e}")
