package example_java.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JavaData {

    public static List<JavaPerson> sampleData() throws IOException {
        List ret = new ArrayList<JavaPerson>();
        sampleDataAsStrings().stream().map(line -> {
            String[] parts = line.split(",");
            return new JavaPerson(parts[0], parts[1], Integer.parseInt(parts[2]), parts[3]);
        });
        return ret;
    }

    public static List<String> sampleDataAsStrings() throws IOException {
        List ret = new ArrayList<String>();
        BufferedReader r = new BufferedReader(new FileReader(new File("src/main/resources/sample_data.csv")));
        String line;
        while ((line=r.readLine())!=null) {
            ret.add(line);
        }
        r.close();
        return ret;
    }
}
