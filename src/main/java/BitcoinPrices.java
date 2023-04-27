import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BitcoinPrices {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> input = env.socketTextStream("localhost", 9000, "\n");

        // Parse the Bitcoin prices and extract the price field
        DataStream<Double> prices = input.map(new MapFunction<String, Double>() {
            @Override
            public Double map(String value) throws Exception {
                return Double.parseDouble(value.split("\n")[0]);
            }
        });

        DataStream<Double> avgPrices = prices.keyBy((Double price) -> 0)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(1)))
                .apply(new AveragePrice());

        avgPrices.print();
        env.execute("Socket Window WordCount");
    }

    public static class AveragePrice implements WindowFunction<Double, Double, Integer, TimeWindow> {
        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Double> input, Collector<Double> out) throws Exception {
            double sum = 0;
            int count = 0;
            for (Double price : input) {
                sum += price;
                count++;
            }
            double avg = sum / count;
            out.collect(avg);
        }
    }
}
