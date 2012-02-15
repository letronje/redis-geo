import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisGeoDemo {
	private static Jedis jedis = new Jedis("localhost");
	
	private static Set<String> getCityNames(String fileName) throws FileNotFoundException, IOException{
		return new HashSet<String>(IOUtils.readLines(new FileReader(fileName)));
	}
	
	private static void createCities(Set<String> cityNames){
		long t1 = System.nanoTime();
		int ctr = 0;
		Pipeline p = jedis.pipelined();
		for(String cityName: cityNames){
			p.set("city_name:"+ctr, cityName);
			p.set("city_id:"+cityName, String.valueOf(ctr));
			p.sync();
			ctr += 1;
		}
		long t2 = System.nanoTime();
		System.out.println(cityNames.size() + " cities added in " + (t2-t1)/1e9 + " secs");
	}
	
	
	private static String getCityIdFromName(String cityName){
		return jedis.get("city_id:" + cityName);
	}
	
	private static String getCityNameFromId(String cityId){
		return jedis.get("city_name:" + cityId);
	}
	
	private static void linkCities(Set<String> cityNamesSet, int numNeighbours){
		ArrayList<String> cityNames = new ArrayList<String>(cityNamesSet);
		List<String> cityNamesDup = (ArrayList<String>) cityNames.clone();
		
		long t1 = System.nanoTime();
		int numLinks = 0;
		
		for(String cityName: cityNames){
			//System.out.print(cityName +  " : ");
			
			String cityId = getCityIdFromName(cityName);
			
			Collections.shuffle(cityNamesDup);
			List<String> neighbourNames = new ArrayList<String>(cityNamesDup).subList(0, numNeighbours);
			neighbourNames.remove(cityName);
			
			//System.out.println(neighbourNames);
			
			
			Pipeline p = jedis.pipelined();
			
			for(String neighbourName: neighbourNames){
				String neighbourId = getCityIdFromName(neighbourName);
				
				p.sadd("neighbours:" + cityId, neighbourId);
				p.sadd("neighbours:" + neighbourId, cityId);
				p.sync();
				
				numLinks ++;
			}
			
		}
			
		long t2 = System.nanoTime();
		System.out.println(numLinks + " edges added in " + (t2-t1)/1e9 + " secs");
	}
	
	private static void printNeighbours(Set<String> cityNames){
		DescriptiveStatistics stats = new DescriptiveStatistics();

		for(String cityName: cityNames){
			long t1 = System.nanoTime();
			//System.out.print(cityName + " : ");
			String cityId = getCityIdFromName(cityName);
			
			Set<String> neighbourIds = jedis.smembers("neighbours:" + cityId);
			List<String> neighbours = new ArrayList<String>();
			
			for(String neighbourId : neighbourIds){
				neighbours.add(getCityNameFromId(neighbourId));
			}
			//System.out.println(neighbours);
			
			long t2 = System.nanoTime();
			long time = t2-t1;
			stats.addValue(time);
		}
		System.out.print("min : " + stats.getMin()/1e6);
		System.out.print(", avg : " + stats.getMean()/1e6);
		System.out.print(", max : " + stats.getMax()/1e6);
		System.out.println(", median : " + stats.getPercentile(50)/1e6);
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		jedis.flushAll();
		
		int numNeighbours = 10;
		int numCities = 10000;
		
		int numUniqueCities = 1000;
		String cityNamesFilePath = args[0];
		List<String> uniqueCityNames = new ArrayList<String>(getCityNames(cityNamesFilePath));
		System.out.println("Found " + uniqueCityNames.size() + " cities in " + cityNamesFilePath);
		int times = numCities/numUniqueCities;
		
		List<String> allCityNames = new ArrayList<String>(); 
		while(times-- > 0){
			for(String c: uniqueCityNames){
				allCityNames.add(c + "_" + times);
			}
		}
		
		Collections.shuffle(allCityNames);
		Set<String> cityNames = new HashSet<String>(allCityNames.subList(0, numCities));
				
		createCities(cityNames);
		linkCities(cityNames, numNeighbours);
		
		int numTimesPrint = 10;
		
		System.out.println();
		System.out.println("Fetching adjacent cities for all cities : ");
		
		for(int i=1;i<=numTimesPrint;i++){
			System.out.print("Run #" + i + " ");
			printNeighbours(cityNames);
		}
	}
}
