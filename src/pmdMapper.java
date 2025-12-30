package pmd;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper:
 * Groups records by Outcome (0 = non-diabetic, 1 = diabetic)
 * Emits clinical values for aggregation
 */
public class pmdMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip empty lines or header
        if (line.isEmpty() || line.toLowerCase().startsWith("pregnancies"))
            return;

        String[] parts = line.split(",");
        if (parts.length < 9)
            return;

        try {
            double pregnancies = Double.parseDouble(parts[0].trim());
            double glucose = Double.parseDouble(parts[1].trim());
            double bmi = Double.parseDouble(parts[5].trim());
            double age = Double.parseDouble(parts[7].trim());
            String outcome = parts[8].trim(); // 0 or 1

            outKey.set(outcome);
            outVal.set(pregnancies + "|" + glucose + "|" + bmi + "|" + age);

            context.write(outKey, outVal);

        } catch (NumberFormatException e) {
            // Ignore malformed rows
        }
    }
}