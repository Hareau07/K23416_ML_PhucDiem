# sakila_tasks.py
# pip install mysql-connector-python pandas scikit-learn openpyxl matplotlib
import pandas as pd
import mysql.connector as mc
from contextlib import contextmanager
from sklearn.cluster import KMeans

# ==== Cấu hình MySQL (đổi mật khẩu cho đúng máy chị) ====
CFG = dict(host="127.0.0.1", port=3306, user="Phuc_Diem",
           password="Huyndiem07@", database="sakila")

@contextmanager
def conn():
    cn = mc.connect(**CFG)
    try:
        yield cn
    finally:
        cn.close()

def q(sql, params=()):
    with conn() as cn:
        return pd.read_sql(sql, cn, params=params)

# (1) Phân loại KH theo TÊN PHIM
def customers_by_film(title: str) -> pd.DataFrame:
    sql = """
        SELECT f.title AS film_title,
               c.customer_id,
               CONCAT(c.first_name,' ',c.last_name) AS customer_name,
               c.email,
               COUNT(r.rental_id) AS times_rented
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f      ON i.film_id      = f.film_id
        JOIN customer c  ON r.customer_id  = c.customer_id
        WHERE f.title = %s
        GROUP BY f.title, c.customer_id, customer_name, c.email
        ORDER BY customer_name;
    """
    return q(sql, (title,))

# (2) Phân loại KH theo CATEGORY (loại trùng)
def customers_by_category(cat_name: str) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT
               cat.name AS category_name,
               c.customer_id,
               CONCAT(c.first_name,' ',c.last_name) AS customer_name,
               c.email
        FROM rental r
        JOIN inventory i      ON r.inventory_id = i.inventory_id
        JOIN film f           ON i.film_id      = f.film_id
        JOIN film_category fc ON f.film_id      = fc.film_id
        JOIN category cat     ON fc.category_id = cat.category_id
        JOIN customer c       ON r.customer_id  = c.customer_id
        WHERE cat.name = %s
        ORDER BY customer_name;
    """
    return q(sql, (cat_name,))

# (3) K-Means: gom cụm khách hàng theo hành vi thuê phim
def build_customer_features() -> pd.DataFrame:
    # Chỉ dùng: customer, inventory, rental, film (đúng đề; thêm film để tính rental_rate)
    sql = """
        SELECT
            c.customer_id,
            CONCAT(c.first_name,' ',c.last_name) AS customer_name,
            COUNT(r.rental_id)                           AS total_rentals,
            COUNT(DISTINCT i.film_id)                    AS unique_films,
            AVG(f.rental_rate)                           AS avg_rental_rate
        FROM customer c
        LEFT JOIN rental r   ON c.customer_id = r.customer_id
        LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
        LEFT JOIN film f       ON i.film_id      = f.film_id
        GROUP BY c.customer_id, customer_name;
    """
    df = q(sql)
    df["total_rentals"]   = pd.to_numeric(df["total_rentals"]).fillna(0)
    df["unique_films"]    = pd.to_numeric(df["unique_films"]).fillna(0)
    df["avg_rental_rate"] = pd.to_numeric(df["avg_rental_rate"]).fillna(0.0)
    # Tỉ lệ lặp lại (>=1 nếu có thuê nhiều lần cùng phim)
    df["repeat_ratio"] = (df["total_rentals"] / df["unique_films"]).replace([float("inf")], 0).fillna(0)
    return df

def kmeans_cluster(k: int = 4) -> pd.DataFrame:
    df = build_customer_features()
    X = df[["total_rentals", "unique_films", "avg_rental_rate", "repeat_ratio"]].values
    model = KMeans(n_clusters=k, init="k-means++", n_init=10, random_state=42)
    df["cluster"] = model.fit_predict(X)
    return df

# ---- Demo nhanh khi chạy file ----
if __name__ == "__main__":
    # (1) Theo TÊN PHIM
    film_df = customers_by_film("ACE GOLDFINGER")
    print("\n[1] KH thuê phim 'ACE GOLDFINGER' (kèm số lần thuê):")
    print(film_df.head(15).to_string(index=False))
    # (2) Theo CATEGORY
    cat_df = customers_by_category("Action")
    print("\n[2] KH thuê phim thuộc Category 'Action' (unique khách):")
    print(cat_df.head(15).to_string(index=False))
    # (3) K-Means
    k = 4
    clusters = kmeans_cluster(k)
    print(f"\n[3] K-Means (k={k}) — tóm tắt theo cụm:")
    summary = clusters.groupby("cluster")[["total_rentals","unique_films","avg_rental_rate","repeat_ratio"]].mean().round(2)
    summary["so_khach"] = clusters.groupby("cluster")["customer_id"].count()
    print(summary.to_string())
    # Top 5 khách ở mỗi cụm
    # In toàn bộ khách hàng theo từng cụm (có thể đặt limit nếu muốn)
    limit = None  # ví dụ: 20 để chỉ in 20 dòng đầu; None = in tất cả

    for cid in sorted(clusters["cluster"].unique()):
        sub = (clusters[clusters["cluster"] == cid]
        .sort_values(["total_rentals", "unique_films"], ascending=False)
        [["customer_id", "customer_name", "total_rentals", "unique_films", "repeat_ratio"]])
        if limit is not None:
            sub = sub.head(limit)
        print(f"\n-- Cụm {cid}: {len(sub)} khách (hiển thị {'tất cả' if limit is None else limit}) --")
        print(sub.to_string(index=False))
# === THÊM TỪ ĐÂY XUỐNG ===
from flask import Flask, render_template_string, request
import io, base64
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

app = Flask(__name__)

HTML = """
<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <title>Sakila Analytics</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
  <h3 class="text-primary text-center mb-4">Phân tích khách hàng – CSDL Sakila</h3>

  <form method="POST" class="row gy-2 gx-3 align-items-end mb-3">
    <div class="col-md-3">
      <label class="form-label">Chế độ</label>
      <select name="mode" class="form-select" onchange="this.form.submit()">
        <option value="cluster" {{ 'selected' if mode=='cluster' else '' }}>Phân cụm K-Means</option>
        <option value="film"    {{ 'selected' if mode=='film'    else '' }}>Theo tên phim</option>
        <option value="category"{{ 'selected' if mode=='category'else '' }}>Theo thể loại</option>
      </select>
    </div>

    {% if mode=='film' %}
      <div class="col-md-6">
        <label class="form-label">Tên phim</label>
        <input name="film_title" class="form-control" placeholder="VD: ACE GOLDFINGER"
               value="{{ request.form.get('film_title','') }}">
      </div>
    {% elif mode=='category' %}
      <div class="col-md-6">
        <label class="form-label">Thể loại</label>
        <input name="category_name" class="form-control" placeholder="VD: Action"
               value="{{ request.form.get('category_name','') }}">
      </div>
    {% else %}
      <div class="col-md-2">
        <label class="form-label">K cụm</label>
        <input type="number" min="2" max="10" name="k_value" class="form-control"
               value="{{ request.form.get('k_value',4) }}">
      </div>
      <div class="col-md-6">
        <label class="form-label">Tìm khách (Tên/ID)</label>
        <input name="search" class="form-control" placeholder="Nhập để lọc trên bảng"
               value="{{ request.form.get('search','') }}">
      </div>
    {% endif %}

    <div class="col-md-2">
      <button class="btn btn-primary w-100">Xem kết quả</button>
    </div>
  </form>

  {% if mode=='cluster' %}
    {% if image %}
      <div class="text-center mb-4">
        <img class="img-fluid border rounded shadow-sm"
             src="data:image/png;base64,{{ image }}">
      </div>
    {% endif %}

    {% if summary %}
      <div class="row g-3">
        {% for r in summary %}
          <div class="col-md-4">
            <div class="card shadow-sm">
              <div class="card-body">
                <h5 class="card-title">Cụm {{ r['Cluster'] }}</h5>
                <div class="small text-muted">Số KH: <b>{{ r['Customers'] }}</b></div>
                <div class="small text-muted">Avg Rentals: <b>{{ '%.2f'|format(r['AvgRentals']) }}</b></div>
                <div class="small text-muted">Avg Unique Films: <b>{{ '%.2f'|format(r['AvgUnique']) }}</b></div>
              </div>
            </div>
          </div>
        {% endfor %}
      </div>

      <ul class="nav nav-tabs mt-4">
        {% for cid in clusters.keys()|list|sort %}
          <li class="nav-item">
            <button class="nav-link {% if loop.first %}active{% endif %}" data-bs-toggle="tab" data-bs-target="#pane-{{ cid }}">
              Cụm {{ cid }}
            </button>
          </li>
        {% endfor %}
      </ul>

      <div class="tab-content border border-top-0 p-3 bg-white">
        {% for cid, data in clusters.items()|sort %}
          <div id="pane-{{ cid }}" class="tab-pane fade {% if loop.first %}show active{% endif %}">
            <table id="tbl-{{ cid }}" class="table table-striped table-bordered table-sm">
              <thead class="table-light">
                <tr>
                  {% for c in data.columns %}<th>{{ c }}</th>{% endfor %}
                </tr>
              </thead>
              <tbody>
                {% for row in data.rows %}
                  <tr>
                    {% for c in data.columns %}<td>{{ row[c] }}</td>{% endfor %}
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
        {% endfor %}
      </div>
    {% endif %}
  {% else %}
    {% if not result_df.empty %}
      <table id="result" class="table table-bordered table-striped">
        <thead class="table-primary">
          <tr>{% for c in result_df.columns %}<th>{{ c }}</th>{% endfor %}</tr>
        </thead>
        <tbody>
          {% for _, r in result_df.iterrows() %}
            <tr>{% for v in r %}<td>{{ v }}</td>{% endfor %}</tr>
          {% endfor %}
        </tbody>
      </table>
    {% endif %}
  {% endif %}
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"></script>
<script>
$(function(){
  {% if mode=='cluster' %}
    {% for cid in clusters.keys()|list|sort %}
      $("#tbl-{{ cid }}").DataTable({
        pageLength: 25,
        lengthMenu: [10,25,50,100],
        order: [[2,'desc'], [3,'desc']] // TotalRentals, UniqueFilms
      });
    {% endfor %}
  {% else %}
    $("#result").DataTable({pageLength:25, lengthMenu:[10,25,50,100]});
  {% endif %}
});
</script>
</body>
</html>
"""

@app.route("/", methods=["GET","POST"])
def index():
    mode = request.form.get("mode","cluster")
    image, result_df, clusters, summary = None, pd.DataFrame(), {}, []

    if mode == "film":
        title = request.form.get("film_title","").strip()
        if title:
            result_df = customers_by_film(title)

    elif mode == "category":
        cat = request.form.get("category_name","").strip()
        if cat:
            result_df = customers_by_category(cat)

    else:  # cluster
        k = int(request.form.get("k_value", 4))
        df = kmeans_cluster(k)

        # lọc nhanh theo tên/ID
        s = request.form.get("search","").strip()
        if s:
            df = df[df["customer_name"].str.lower().str.contains(s.lower()) |
                    df["customer_id"].astype(str).str.contains(s)]

        # scatter (TotalRentals vs UniqueFilms)
        fig, ax = plt.subplots(figsize=(7,5))
        ax.scatter(df["total_rentals"], df["unique_films"], c=df["cluster"], s=60)
        ax.set_xlabel("Total Rentals"); ax.set_ylabel("Unique Films")
        ax.set_title(f"K-Means Customer Clusters (K={k})")
        buf = io.BytesIO(); fig.savefig(buf, format="png"); buf.seek(0)
        image = base64.b64encode(buf.read()).decode("utf-8"); plt.close(fig)

        # cards tóm tắt
        g = (df.groupby("cluster")[["total_rentals","unique_films"]]
               .agg(Customers=("total_rentals","size"),
                    AvgRentals=("total_rentals","mean"),
                    AvgUnique=("unique_films","mean"))
               .round(2).reset_index())
        summary = [{"Cluster": int(r["cluster"]),
                    "Customers": int(r["Customers"]),
                    "AvgRentals": r["AvgRentals"],
                    "AvgPayment": r["AvgUnique"],   # dùng AvgUnique làm chỉ báo thứ 2
                    "AvgUnique": r["AvgUnique"]} for _, r in g.iterrows()]

        # bảng theo cụm
        for cid in sorted(df["cluster"].unique()):
            sub = (df[df["cluster"] == cid]
                  .sort_values(["total_rentals","unique_films"], ascending=False)
                  [["customer_id","customer_name","total_rentals","unique_films","avg_rental_rate","repeat_ratio"]])
            clusters[int(cid)] = {
                "columns": list(sub.columns),
                "rows": sub.to_dict(orient="records")
            }

    return render_template_string(HTML, mode=mode, image=image,
                                  clusters=clusters, summary=summary,
                                  result_df=result_df, request=request)

# 1) GIỮ NGUYÊN: CFG, conn(), q(), customers_by_film(), customers_by_category(),
# build_customer_features(), kmeans_cluster()  (y như chị đang có)

# 2) GIỮ NGUYÊN: phần Flask app + HTML template (chị đã dán xong)

# 3) KHỐI MAIN: chạy Flask + tự mở trình duyệt
import threading, webbrowser

def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000")

if __name__ == "__main__":
    print("⏳ Đang khởi động Flask server...")
    threading.Timer(1.5, open_browser).start()  # mở sau 1.5s để server sẵn sàng
    app.run(debug=True, use_reloader=False, port=5000)

