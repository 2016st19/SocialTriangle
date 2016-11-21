/**
 * Created by 2016st19 on 11/21/16.
 */
public class Driver {
    public static void main(String[] args) throws Exception{
        String inputPath = args[0];
        String outPath = args[1] + "/buf/Dereplication";
        Dereplication.main(inputPath, outPath);
        inputPath = outPath;
        outPath = args[1] + "/buf/FindPair";
        FindPair.main(inputPath, outPath);
        inputPath = outPath;
        outPath = args[1] + "/buf/TriCount";
        TriCount.main(inputPath, outPath);
        inputPath = outPath;
        outPath = args[1] + "/result";
        CollectCount.main(inputPath, outPath);
    }
}
