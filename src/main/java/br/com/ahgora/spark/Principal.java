package br.com.ahgora.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Principal {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("start-spark").setMaster("local");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			// Dados dos táxis de nova york disponibilizado pela url:
			// http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
			JavaRDD<String> rdd = sc.textFile(System.getProperty("user.home")+"/taxi_data/yellow_tripdata_2017-01.csv");

			// imprime o número de linhas do arquivo
			System.out.println("Número total de linhas do arquivo:" + rdd.count());

			// remove o cabeçalho e linhas em branco do arquivo
			JavaRDD<String> dataRdd = rdd
					.filter(line -> !line.toLowerCase().startsWith("vendorid") && !line.trim().isEmpty());
			long totalRegistros = dataRdd.count();
			System.out.println("Número registros no arquivo:" + totalRegistros);

			// primeiro faz uma projecao somente na coluna com as distancias da viagem,
			// depois soma todas as distâncias
			Double totalDistanciaViagens = dataRdd.map(s -> {
				String distancia = s.split(",")[4];
				distancia = distancia.startsWith(".") ? "0" + distancia : distancia;
				return Double.valueOf(distancia);
			}).reduce((total, distancia) -> {
				total = total == null ? 0 : total;
				return total + distancia;
			});
			// por fim calcula a média das viagens realizada no mês de janeiro de 2017
			System.out.println(
					"Média das viagens realizada no mês de janeiro de 2017: " + totalDistanciaViagens / totalRegistros);
		}
	}

}
