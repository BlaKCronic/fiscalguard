// FiscalGuard - Fase 5: comparativa MapReduce vs Spark.
//
// Monto facturado (CFDI de ingreso vigentes) y número de CFDI por RFC emisor, leyendo la zona
// raw (CSV) de HDFS. Es la misma lógica que spark_jobs/08_spark_vs_mapreduce.py, para comparar
// tiempos de los dos motores sobre los mismos datos.
//
// Columnas del CSV: uuid,rfc_emisor,rfc_receptor,fecha,subtotal,iva,total,
//                   tipo_comprobante,uso_cfdi,forma_pago,clave_prod_serv,estatus
// Salida (texto):   rfc_emisor <TAB> num_cfdi <TAB> monto_centavos
// El monto se suma en centavos (long) para que el resultado sea exacto y comparable con Spark.
//
// Compilar y correr (dentro del contenedor resourcemanager): ver scripts/compare_mr_spark.ps1
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MontoPorEmisor {

  static final Pattern RFC = Pattern.compile("^[A-ZÑ&]{3,4}[0-9]{6}[A-Z0-9]{3}$");

  /** Emite (rfc_emisor, "1,centavos") por cada CFDI de ingreso vigente con datos válidos. */
  public static class MapperCfdi extends Mapper<LongWritable, Text, Text, Text> {
    private final Text rfc = new Text();
    private final Text val = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context ctx) throws IOException, InterruptedException {
      String[] f = line.toString().split(",", -1);
      if (f.length < 12 || "uuid".equals(f[0])) { ctx.getCounter("fiscalguard", "lineas_descartadas").increment(1); return; }
      String emisor = f[1].trim().toUpperCase();
      if (!"I".equals(f[7].trim()) || !"Vigente".equals(f[11].trim()) || !RFC.matcher(emisor).matches()) {
        ctx.getCounter("fiscalguard", "lineas_descartadas").increment(1);
        return;
      }
      double total;
      try { total = Double.parseDouble(f[6].trim()); } catch (NumberFormatException e) {
        ctx.getCounter("fiscalguard", "lineas_descartadas").increment(1);
        return;
      }
      if (!(total > 0)) { ctx.getCounter("fiscalguard", "lineas_descartadas").increment(1); return; }
      rfc.set(emisor);
      val.set("1," + Math.round(total * 100));
      ctx.write(rfc, val);
      ctx.getCounter("fiscalguard", "cfdi_validos").increment(1);
    }
  }

  /** Combiner: suma parciales en el mismo formato "n,centavos" (reduce el tráfico del shuffle). */
  public static class Combiner extends Reducer<Text, Text, Text, Text> {
    private final Text out = new Text();
    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
      long n = 0, c = 0;
      for (Text v : vals) { String s = v.toString(); int i = s.indexOf(','); n += Long.parseLong(s.substring(0, i)); c += Long.parseLong(s.substring(i + 1)); }
      out.set(n + "," + c);
      ctx.write(k, out);
    }
  }

  /** Reducer final: "rfc  num_cfdi  monto_centavos". */
  public static class Sumador extends Reducer<Text, Text, Text, Text> {
    private final Text out = new Text();
    @Override
    protected void reduce(Text k, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
      long n = 0, c = 0;
      for (Text v : vals) { String s = v.toString(); int i = s.indexOf(','); n += Long.parseLong(s.substring(0, i)); c += Long.parseLong(s.substring(i + 1)); }
      out.set(n + "\t" + c);
      ctx.write(k, out);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) { System.err.println("Uso: MontoPorEmisor <entrada> <salida> [reducers]"); System.exit(2); }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "fiscalguard-mr-monto-por-emisor");
    job.setJarByClass(MontoPorEmisor.class);
    job.setMapperClass(MapperCfdi.class);
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(Sumador.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 2);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    long t0 = System.currentTimeMillis();
    boolean ok = job.waitForCompletion(true);
    double seg = (System.currentTimeMillis() - t0) / 1000.0;
    System.out.printf("%nMAPREDUCE_SEGUNDOS=%.1f%n", seg);
    System.out.println("MAPREDUCE_CFDI_VALIDOS=" + job.getCounters().findCounter("fiscalguard", "cfdi_validos").getValue());
    System.exit(ok ? 0 : 1);
  }
}
