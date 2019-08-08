package com.gavin.bigdata.hdfs;

public class WordCountMapper implements IHDFSMapper {

    @Override
    public void map(String line, HDFSContext context) {
        String[] words = line.split(" ");
        for (String word : words) {
            Object value = context.get(word);
            // 表示该单词没有出现过
            if (null == value) {
                context.write(word, 1);
            } else {
                context.write(word, Integer.parseInt(value.toString()) + 1);
            }
        }
    }
}
