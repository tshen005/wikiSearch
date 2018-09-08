package edu.ucr.cs242.mixer.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class IndexWritable implements Writable {
    private int docId;
    private int[] frequency;
    private int[] position;

    public int getDocId() {
        return docId;
    }

    public int[] getFrequency() {
        return frequency;
    }

    public int[] getPosition() {
        return position;
    }

    public IndexWritable() {
        docId = -1;
        frequency = null;
        position = null;
    }

    public IndexWritable(int docId, int[] frequency, int[] position) {
        this.docId = docId;
        this.frequency = frequency;
        this.position = position;
    }

    private int[] readIntArray(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        int[] array = new int[length];

        for (int i = 0; i < length; i++) {
            array[i] = dataInput.readInt();
        }
        return array;
    }

    private void writeIntArray(DataOutput dataOutput, int[] array) throws IOException {
        dataOutput.writeInt(array.length);
        for (int v : array) {
            dataOutput.writeInt(v);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docId = dataInput.readInt();
        frequency = readIntArray(dataInput);
        position = readIntArray(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.docId);
        writeIntArray(dataOutput, this.frequency);
        writeIntArray(dataOutput, position);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(docId);
        sb.append(Arrays.stream(frequency)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(",", ":", "|")));
        sb.append(Arrays.stream(position)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(",")));
        return sb.toString();
    }
}
