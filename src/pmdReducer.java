package pmd;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer:
 * Calculates record count and averages per Outcome
 */
public class  extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalRecords = 0;

        double glucoseSum = 0.0, bmiSum = 0.0, ageSum = 0.0;
        long glucoseCount = 0, bmiCount = 0, ageCount = 0;

        for (Text val : values) {
            String[] fields = val.toString().split("\\|");
            if (fields.length != 4)
                continue;

            totalRecords++;

            double glucose = Double.parseDouble(fields[1]);
            if (glucose > 0) {
                glucoseSum += glucose;
                glucoseCount++;
            }

            double bmi = Double.parseDouble(fields[2]);
            if (bmi > 0) {
                bmiSum += bmi;
                bmiCount++;
            }

            double age = Double.parseDouble(fields[3]);
            if (age > 0) {
                ageSum += age;
                ageCount++;
            }
        }

        double avgGlucose = (glucoseCount == 0) ? 0 : glucoseSum / glucoseCount;
        double avgBMI = (bmiCount == 0) ? 0 : bmiSum / bmiCount;
        double avgAge = (ageCount == 0) ? 0 : ageSum / ageCount;

        String result =
                "count=" + totalRecords +
                "\tavgGlucose=" + String.format("%.2f", avgGlucose) +
                "\tavgBMI=" + String.format("%.2f", avgBMI) +
                "\tavgAge=" + String.format("%.2f", avgAge);

        context.write(key, new Text(result));
    }
}

