#!/usr/bin/env python3
"""
FiscalGuard - Generador de datos sintéticos v4 (2014-2025, series de tiempo realistas).

Produce, en la estructura de la zona RAW del Data Lake:
  <out>/raw/padron/padron.csv            padrón sintético de contribuyentes
  <out>/raw/cfdi/cfdi_part_XXXXX.csv     CFDI sintéticos (o Parquet particionado con --format parquet)
  <out>/raw/lista_69b/lista_69b.csv      lista 69-B sintética (solo una fracción de los EFOS "conocidos")
  <out>/ground_truth/efos_verdad.csv     verdad terreno (para evaluar recall/precisión)
  <out>/ground_truth/redes_verdad.csv    redes de facturación circular inyectadas

Mejoras de la v2 respecto a la v1 (observación del profesor: las series mensuales eran idénticas):
  - Estacionalidad mensual y por sector (cierre de año, Buen Fin, temporada de construcción y agrícola).
  - Tendencia de crecimiento (~5 % anual), picos de fin de mes, menos facturación en fin de semana
    y horario laboral; ruido aleatorio mes a mes.
  - Los EFOS nacen de forma ESCALONADA a lo largo de todo el periodo (antes casi todos al inicio),
    con rampa de actividad y corte abrupto, así su serie ya no copia la del total.
  - Montos de EFOS y de anillos menos extremos (se traslapan más con los normales) y menor peso
    de los EFOS en el volumen: el monto "en riesgo" ya no es ~50 % del total.
  - Los anillos se forman entre EFOS cuyos periodos de operación coinciden.
Mejoras de la v3 (la tendencia mensual aún tenía un "escalón": muchos EFOS arrancaban juntos en
enero de 2024 y concentraban su volumen en pocos meses):
  - El volumen de cada contribuyente es proporcional a los días que opera dentro del periodo:
    una empresa activa 4 meses ya no factura en 4 meses lo mismo que otra en 24.
  - Los EFOS que nacieron antes del periodo ya no "arrancan" todos el 1 de enero: su ventana se
    recorta, no se desplaza. En cualquier mes hay un número parecido de EFOS activos (más oleadas).
  - Ruido mensual independiente por sector, eventos puntuales (meses atípicos) y anillos con montos
    más moderados: el monto en riesgo queda en ~5-15 % del total y varía mes a mes.
Mejoras de la v4 (periodo largo, por defecto 2014-2025 = obligatoriedad general del CFDI y del art. 69-B):
  - Altas desde 1990; ~25 % de las empresas normales cierra durante el periodo y otras abren en él.
  - Régimen de personas físicas coherente con 2025: 612 o 626 RESICO (el RIF desapareció en 2022).
  - Choque de la pandemia (abr-sep 2020) y recuperación en 2021.
  - La intensidad de los EFOS se ajusta a la duración del periodo para que su peso mensual no
    dependa de cuántos años se generen; más "oleadas" de creación en periodos largos.
Mismo esquema de columnas y archivos que la v1: el resto del pipeline no cambia.
"""
import argparse
import string
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

SECTORES = ["Comercio al por mayor", "Comercio al por menor", "Construcción",
            "Servicios profesionales", "Transporte", "Manufactura",
            "Publicidad y TI", "Agricultura"]
# 5 claves de producto/servicio (sintéticas, estilo SAT) por sector
CODIGOS = np.array([
    ["43211500", "43232300", "46171600", "52161500", "56101500"],
    ["50202300", "50181900", "53102700", "47131800", "52121500"],
    ["72101500", "72151200", "30161500", "31161500", "72141000"],
    ["80101500", "80111600", "84111500", "80121600", "81111800"],
    ["78101800", "78181500", "78101500", "25101500", "78111800"],
    ["31201600", "27111500", "23151800", "40141600", "39121000"],
    ["82101500", "43231500", "81112000", "82111900", "81111500"],
    ["10101500", "10151500", "10171500", "10191500", "10161500"],
])
ESTADOS = ["Aguascalientes", "Baja California", "Baja California Sur", "Campeche", "Chiapas",
           "Chihuahua", "Ciudad de México", "Coahuila", "Colima", "Durango", "Guanajuato",
           "Guerrero", "Hidalgo", "Jalisco", "México", "Michoacán", "Morelos", "Nayarit",
           "Nuevo León", "Oaxaca", "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí",
           "Sinaloa", "Sonora", "Tabasco", "Tamaulipas", "Tlaxcala", "Veracruz", "Yucatán",
           "Zacatecas"]
USO = np.array(["G01", "G03", "P01", "S01", "I01", "D01"])
USO_P = [0.15, 0.55, 0.1, 0.1, 0.05, 0.05]
FORMA = np.array(["03", "04", "01", "28", "99"])
FORMA_P = [0.55, 0.15, 0.1, 0.1, 0.1]
PREF = ["Comercializadora", "Grupo", "Servicios", "Distribuidora", "Constructora", "Soluciones",
        "Consultores", "Industrias", "Transportes", "Agroproductos"]
CORE = ["Bajío", "Aztlán", "Delta", "Omega", "Cumbre", "Nova", "Titán", "Alfa", "Sigma",
        "Horizonte", "Meridiano", "Zafiro", "Pionero", "Vértice", "Andes"]
SUF = ["S.A. de C.V.", "S. de R.L. de C.V.", "S.A.P.I. de C.V."]
NOM = ["Juan", "María", "José", "Ana", "Luis", "Carmen", "Pedro", "Laura", "Jorge", "Rosa"]
APE = ["Hernández", "García", "Martínez", "López", "González", "Pérez", "Rodríguez", "Sánchez",
       "Ramírez", "Flores"]
PRIME = 7919
DAY = 86400
EFOS_WEIGHT = 4.5   # intensidad mensual de un EFOS frente a un normal (v3 escala por días activos)

# --- estacionalidad (multiplicadores por mes, enero..diciembre) ---
MES_GLOBAL = np.array([0.85, 0.92, 1.00, 0.95, 1.02, 1.00, 0.97, 1.00, 1.03, 1.05, 1.12, 1.18])
MES_SECTOR = {
    "Construcción": [1.00, 1.00, 1.05, 1.05, 1.05, 0.95, 0.88, 0.88, 0.90, 1.02, 1.08, 1.12],
    "Agricultura": [0.90, 0.90, 1.00, 1.10, 1.15, 1.00, 0.90, 0.90, 0.95, 1.15, 1.15, 1.00],
    "Comercio al por menor": [0.80, 0.85, 0.95, 0.95, 1.00, 0.95, 0.95, 1.00, 1.00, 1.05, 1.25, 1.40],
}
# pandemia: caída abr-jun 2020 y recuperación gradual
COVID = {(2020, 3): 0.93, (2020, 4): 0.72, (2020, 5): 0.70, (2020, 6): 0.80, (2020, 7): 0.86,
         (2020, 8): 0.90, (2020, 9): 0.94, (2020, 10): 0.97}
HORA = np.array([.2] * 7 + [1, 3, 6, 7, 7, 6, 5, 6, 7, 6, 4, 2, 1, .7, .3, .3, .3])
HORA_P = HORA / HORA.sum()


def make_rfcs(rng, tipo, alta):
    """RFC de 12 (moral) o 13 (física) caracteres; la fecha embebida es la de alta."""
    L = np.array(list(string.ascii_uppercase))
    H = np.array(list(string.ascii_uppercase + string.digits))
    dates = pd.DatetimeIndex(alta).strftime("%y%m%d")
    letters = L[rng.integers(0, 26, (len(tipo), 4))]
    seen, out = set(), []
    for i, t in enumerate(tipo):
        pre = "".join(letters[i, :3]) if t == "M" else "".join(letters[i])
        while True:
            rfc = pre + dates[i] + "".join(H[rng.integers(0, 36, 3)])
            if rfc not in seen:
                seen.add(rfc)
                out.append(rfc)
                break
    return np.array(out)


def build_day_cdfs(rng, start64, end64):
    """Una CDF de actividad diaria por sector: estacionalidad x tendencia x día de la semana x fin de mes x ruido."""
    days = pd.date_range(start64, end64, freq="D")
    D = len(days)
    mes = days.month.values - 1
    trend = 1.004 ** (np.arange(D) / 30.0)                       # ~5 % anual
    dow = np.array([1, 1, 1, 1, 1, 0.35, 0.15])[days.dayofweek.values]
    fin_mes = np.where(days.day.values >= days.days_in_month.values - 2, 1.4, 1.0)
    ym = days.year.values * 12 + days.month.values
    _, inv = np.unique(ym, return_inverse=True)
    nm = inv.max() + 1
    shock = rng.lognormal(0, 0.07, nm)                            # ruido mensual de la economía
    evento = np.ones(nm)
    k = rng.choice(nm, max(1, nm // 8), replace=False)            # meses atípicos (caídas o repuntes)
    evento[k] = rng.choice([0.8, 0.85, 1.15, 1.25], len(k))
    covid = np.ones(D)
    for (y, m), f in COVID.items():
        covid[(days.year.values == y) & (days.month.values == m)] = f
    cdfs = []
    for s in SECTORES:
        adj = np.array(MES_SECTOR.get(s, [1.0] * 12))[mes]
        s_shock = rng.lognormal(0, 0.08, nm)                      # ruido propio del sector
        w = MES_GLOBAL[mes] * adj * trend * dow * fin_mes * covid * (shock * evento * s_shock)[inv]
        c = np.concatenate([[0.0], np.cumsum(w)])
        cdfs.append(c / c[-1])
    return cdfs, D


def sample_ts(rng, aux, s0, s1, sector, efos=None):
    """Segundos epoch dentro de [s0, s1] siguiendo la actividad diaria del sector (EFOS: rampa y corte abrupto)."""
    n = len(s0)
    D = aux["D"]
    d0 = np.clip((s0 - aux["start_s"]) // DAY, 0, D - 1)
    d1 = np.clip((s1 - aux["start_s"]) // DAY, d0, D - 1)
    frac = rng.random(n)
    if efos is not None and efos.any():
        frac = np.where(efos, rng.beta(1.6, 1.0, n), frac)        # más actividad hacia el final de su ráfaga
    day = np.empty(n, np.int64)
    for s in range(len(SECTORES)):
        m = sector == s
        if not m.any():
            continue
        cdf = aux["cdf"][s]
        lo, hi = cdf[d0[m]], cdf[d1[m] + 1]
        day[m] = np.clip(np.searchsorted(cdf, lo + frac[m] * (hi - lo), side="right") - 1, d0[m], d1[m])
    hora = rng.choice(24, n, p=HORA_P)
    return aux["start_s"] + day * DAY + hora * 3600 + rng.integers(0, 3600, n)


def build_padron(rng, n, efos_ratio, start, end):
    start64, end64 = np.datetime64(start, "D"), np.datetime64(end, "D")
    base = np.datetime64("1990-01-01", "D")
    period_days = int((end64 - start64).astype(int))
    tipo = rng.choice(np.array(["M", "F"]), n, p=[0.45, 0.55])
    efos = np.zeros(n, bool)
    efos[rng.choice(n, max(6, int(n * efos_ratio)), replace=False)] = True
    e = np.where(efos)[0]

    alta = base + rng.integers(0, (end64 - base).astype(int) - 30, n).astype("timedelta64[D]")
    # EFOS: el INICIO de operaciones se reparte en todo el periodo (incluso antes de que empiece),
    # 75 % uniforme + 25 % en 3 "oleadas" (p. ej. antes de cierres fiscales). La fecha de alta es
    # 30-120 días antes. La ventana se RECORTA al periodo (v2 la desplazaba y todos arrancaban en enero).
    ini = (start64 - np.timedelta64(200, "D") - base).astype(int)
    fin = (end64 - np.timedelta64(45, "D") - base).astype(int)
    d = rng.integers(ini, fin, len(e))
    ola = rng.random(len(e)) < 0.25
    n_olas = max(3, period_days // 730)
    centros = rng.integers(ini + 200, fin - 60, n_olas)
    d[ola] = np.clip(centros[rng.integers(0, n_olas, ola.sum())] + rng.normal(0, 30, ola.sum()).astype(int), ini, fin)
    ef_start = base + d.astype("timedelta64[D]")
    alta[e] = ef_start - rng.integers(30, 120, len(e)).astype("timedelta64[D]")
    burst = rng.integers(120, 330, len(e))
    falta = np.maximum(0, (start64 - ef_start).astype(int) + 45)   # al menos 45 días dentro del periodo
    burst = np.maximum(burst, falta)

    op_start = np.maximum(alta, start64)
    op_start[e] = np.maximum(ef_start, start64)
    op_end = np.full(n, end64)
    # ~25 % de las empresas normales cierra durante el periodo (al menos 1 año después de iniciar)
    cierra = (~efos) & (rng.random(n) < 0.25)
    span = np.maximum((end64 - op_start[cierra]).astype(int) - 365, 30)
    op_end[cierra] = op_start[cierra] + (365 + (rng.random(cierra.sum()) * span).astype(int)).astype("timedelta64[D]")
    op_end[e] = np.minimum(ef_start + burst.astype("timedelta64[D]"), end64)
    dias_activos = np.maximum((op_end - op_start).astype(int), 15)

    pool = np.clip(rng.lognormal(3.0, 1.0, n), 1, 3000).astype(int)
    pool[e] = rng.integers(2, 9, len(e))
    w = rng.lognormal(0, 1.5, n)
    w[e] *= EFOS_WEIGHT * max(1.0, 0.65 * period_days / 730)   # intensidad mensual estable con cualquier duración
    w *= dias_activos / dias_activos.max()   # volumen proporcional al tiempo que opera en el periodo
    sector = rng.integers(0, len(SECTORES), n)
    estado = rng.integers(0, len(ESTADOS), n)
    rfc = make_rfcs(rng, tipo, alta)

    nombres = []
    for i in range(n):
        if tipo[i] == "M":
            nombres.append(f"{rng.choice(PREF)} {rng.choice(CORE)} {i % 1000:03d} {rng.choice(SUF)}")
        else:
            nombres.append(f"{rng.choice(NOM)} {rng.choice(APE)} {rng.choice(APE)}")
    # régimen vigente (2025): el RIF (621) desapareció; RESICO (626) existe desde 2022
    reciente = alta >= np.datetime64("2014-01-01", "D")
    pf = np.where(rng.random(n) < np.where(reciente, 0.55, 0.30), "626 RESICO", "612 Actividades Empresariales y Profesionales")
    pm = np.where(rng.random(n) < 0.08, "626 RESICO Personas Morales", "601 General de Ley Personas Morales")
    regimen = np.where(tipo == "M", pm, pf)
    padron = pd.DataFrame({
        "rfc": rfc, "razon_social": nombres, "tipo_persona": tipo, "regimen": regimen,
        "sector": np.array(SECTORES)[sector], "fecha_inicio_operaciones": alta.astype(str),
        "estado": np.array(ESTADOS)[estado],
        "municipio": [f"Mun-{k:03d}" for k in rng.integers(1, 60, n)],
    })
    to_s = lambda a: a.astype("datetime64[s]").astype(np.int64)
    cdfs, D = build_day_cdfs(rng, start64, end64)
    aux = dict(N=n, rfc=rfc, efos=efos, t0=to_s(op_start), t1=to_s(op_end), pool=pool,
               p=w / w.sum(), sector=sector, mismatch=np.where(efos, 0.7, 0.08),
               end_s=int(to_s(np.array(end64))), start_s=int(to_s(np.array(start64))),
               op_end=op_end, cdf=cdfs, D=D)
    return padron, aux


def to_cfdi_df(rng, aux, emi, rec, sub, ts):
    n = len(emi)
    ef = aux["efos"][emi]
    sec = np.where(rng.random(n) < aux["mismatch"][emi],
                   rng.integers(0, len(SECTORES), n), aux["sector"][emi])
    uid = [str(uuid.UUID(int=(int(a) << 64) | int(b), version=4)) for a, b in
           zip(rng.integers(0, 2**63, n, dtype=np.int64), rng.integers(0, 2**63, n, dtype=np.int64))]
    return pd.DataFrame({
        "uuid": uid,
        "rfc_emisor": aux["rfc"][emi],
        "rfc_receptor": aux["rfc"][rec],
        "fecha": pd.to_datetime(ts, unit="s"),
        "subtotal": sub,
        "iva": np.round(sub * 0.16, 2),
        "total": np.round(sub * 1.16, 2),
        "tipo_comprobante": np.where(rng.random(n) < 0.07, "E", "I"),
        "uso_cfdi": rng.choice(USO, n, p=USO_P),
        "forma_pago": rng.choice(FORMA, n, p=FORMA_P),
        "clave_prod_serv": CODIGOS[sec, rng.integers(0, 5, n)],
        "estatus": np.where(rng.random(n) < np.where(ef, 0.08, 0.03), "Cancelado", "Vigente"),
    })


def gen_normal(rng, aux, n):
    N = aux["N"]
    emi = rng.choice(N, size=n, p=aux["p"])
    rec = (emi + 1 + np.floor(rng.random(n) * aux["pool"][emi]).astype(np.int64) * PRIME) % N
    rec = np.where(rec == emi, (rec + 1) % N, rec)
    ef = aux["efos"][emi]
    ts = sample_ts(rng, aux, aux["t0"][emi], aux["t1"][emi], aux["sector"][emi], ef)
    # EFOS: montos más altos pero traslapados con los normales (v1: mediana 36 mil vs 5 mil)
    x = np.where(ef, rng.lognormal(9.9, 1.0, n), rng.lognormal(8.5, 1.3, n))
    # el monto sube levemente en diciembre y con la tendencia (inflación)
    redondo = ef & (rng.random(n) < 0.55)
    x = np.where(redondo, np.maximum(10000, np.round(x / 10000) * 10000), np.round(x, 2))
    return to_cfdi_df(rng, aux, emi, rec, np.clip(x, 50, 5e7), ts)


def pick_members(rng, aux, efos_idx, k):
    """Miembros de un anillo: EFOS cuyos periodos de operación se traslapan con los de un ancla."""
    for _ in range(30):
        a = rng.choice(efos_idx)
        cand = efos_idx[(aux["t0"][efos_idx] < aux["t1"][a] - 30 * DAY) & (aux["t1"][efos_idx] > aux["t0"][a] + 30 * DAY)]
        cand = cand[cand != a]
        if len(cand) >= 2:
            return np.concatenate([[a], rng.choice(cand, min(k - 1, len(cand)), replace=False)]), a
    a = rng.choice(efos_idx)
    return np.concatenate([[a], rng.choice(efos_idx[efos_idx != a], k - 1, replace=False)]), a


def gen_cycles(rng, aux, n_rows):
    """Anillos A->B->C->...->A entre EFOS, con montos casi iguales (ruta del dinero)."""
    efos_idx = np.where(aux["efos"])[0]
    n_net = max(1, int(len(efos_idx) * 0.08))
    ks = rng.integers(3, 7, n_net)
    per_edge = max(5, n_rows // int(ks.sum()))
    E, R, S, T, truth = [], [], [], [], []
    for r, k in enumerate(ks):
        mem, a = pick_members(rng, aux, efos_idx, k)
        rng.shuffle(mem)
        k = len(mem)
        # el anillo opera durante la vida del EFOS "ancla" (v2: se iba casi siempre al inicio del periodo)
        t0, t1 = aux["t0"][a], aux["t1"][a]
        if t1 - t0 < 60 * DAY:
            t0 = max(aux["start_s"], min(t0, aux["end_s"] - 90 * DAY))
            t1 = min(t0 + 90 * DAY, aux["end_s"])
        base = rng.lognormal(10.2, 0.5)            # v1: 12.0 (≈ 160 mil); v2 ≈ 50 mil; v3 ≈ 27 mil
        for j in range(k):
            E.append(np.full(per_edge, mem[j]))
            R.append(np.full(per_edge, mem[(j + 1) % k]))
            S.append(np.round(base * (0.97 ** j) * rng.uniform(0.9, 1.1, per_edge), 2))
            ts = sample_ts(rng, aux, np.full(per_edge, t0), np.full(per_edge, t1),
                           np.full(per_edge, aux["sector"][mem[j]]))
            T.append(ts)
            truth.append((f"RED{r + 1:04d}", aux["rfc"][mem[j]], j))
    df = to_cfdi_df(rng, aux, np.concatenate(E), np.concatenate(R), np.concatenate(S), np.concatenate(T))
    return df, pd.DataFrame(truth, columns=["id_red", "rfc", "orden"])


def ensuciar(rng, df, frac):
    """Duplicados por UUID y RFC receptor inválidos (para el ETL de la Fase 2)."""
    if frac <= 0:
        return df
    dup = df.sample(frac=frac * 0.6, random_state=int(rng.integers(0, 1e9)))
    df = pd.concat([df, dup], ignore_index=True)
    bad = rng.random(len(df)) < frac * 0.4
    df.loc[bad, "rfc_receptor"] = [f"XX{k}" for k in rng.integers(0, 999, bad.sum())]
    return df


def write_chunk(df, k, args, out):
    if args.format == "csv":
        d = out / "raw" / "cfdi"
        d.mkdir(parents=True, exist_ok=True)
        df.to_csv(d / f"cfdi_part_{k:05d}.csv", index=False, date_format="%Y-%m-%dT%H:%M:%S")
    else:
        import pyarrow as pa
        import pyarrow.dataset as ds
        df = df.copy()
        df["anio"] = df["fecha"].dt.year.astype("int32")
        df["mes"] = df["fecha"].dt.month.astype("int32")
        ds.write_dataset(pa.Table.from_pandas(df, preserve_index=False), out / "raw" / "cfdi_parquet",
                         format="parquet", partitioning=["anio", "mes"], partitioning_flavor="hive",
                         basename_template=f"part-{k}-{{i}}.parquet",
                         existing_data_behavior="overwrite_or_ignore")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--contribuyentes", type=int, default=100_000)
    ap.add_argument("--cfdi", type=int, default=20_000_000, help="total de CFDI (propuesta original: 50,000,000)")
    ap.add_argument("--efos-ratio", type=float, default=0.02)
    ap.add_argument("--known-ratio", type=float, default=0.8, help="fracción de EFOS que aparece en la lista 69-B")
    ap.add_argument("--cycle-share", type=float, default=0.03, help="fracción de CFDI en ciclos circulares")
    ap.add_argument("--dirty", type=float, default=0.005)
    ap.add_argument("--start", default="2014-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--chunk", type=int, default=1_000_000)
    ap.add_argument("--format", choices=["csv", "parquet"], default="csv")
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    out = Path(args.out)
    for sub in ("raw/padron", "raw/lista_69b", "ground_truth"):
        (out / sub).mkdir(parents=True, exist_ok=True)

    padron, aux = build_padron(rng, args.contribuyentes, args.efos_ratio, args.start, args.end)
    padron.to_csv(out / "raw/padron/padron.csv", index=False)

    efos = aux["efos"]
    conocido = efos & (rng.random(len(efos)) < args.known_ratio)
    c = np.where(conocido)[0]
    pd.DataFrame({
        "rfc": aux["rfc"][c], "nombre": padron["razon_social"].to_numpy()[c],
        "situacion": rng.choice(["Presunto", "Definitivo"], len(c), p=[0.4, 0.6]),
        "fecha_publicacion": (aux["op_end"][c] + rng.integers(30, 200, len(c)).astype("timedelta64[D]")).astype(str),
    }).to_csv(out / "raw/lista_69b/lista_69b.csv", index=False)
    pd.DataFrame({"rfc": aux["rfc"][efos], "es_efos": 1,
                  "en_lista_69b": conocido[efos].astype(int)}).to_csv(out / "ground_truth/efos_verdad.csv", index=False)

    n_cyc = int(args.cfdi * args.cycle_share)
    cyc, truth = gen_cycles(rng, aux, n_cyc)
    truth.to_csv(out / "ground_truth/redes_verdad.csv", index=False)
    k = 0
    write_chunk(ensuciar(rng, cyc, args.dirty), k, args, out)
    remaining = args.cfdi - len(cyc)
    total_normal = remaining
    while remaining > 0:
        k += 1
        n = min(args.chunk, remaining)
        write_chunk(ensuciar(rng, gen_normal(rng, aux, n), args.dirty), k, args, out)
        remaining -= n
        print(f"  chunk {k}: {total_normal - remaining:,} CFDI normales", flush=True)

    print(f"Listo: {len(padron):,} contribuyentes ({efos.sum():,} EFOS, {conocido.sum():,} en 69-B), "
          f"{args.cfdi:,} CFDI en {out}/raw/")


if __name__ == "__main__":
    main()
