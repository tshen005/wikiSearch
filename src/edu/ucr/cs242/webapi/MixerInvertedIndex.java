package edu.ucr.cs242.webapi;

import java.util.List;
import java.util.Map;

public class MixerInvertedIndex {
    private final int docId;
    private final List<Integer> frequency;
    // <FieldId, [Position]>
    private final List<List<Integer>> position;

    public int getDocId() {
        return docId;
    }

    public List<Integer> getFrequency() {
        return frequency;
    }

    public List<List<Integer>> getPosition() {
        return position;
    }

    public MixerInvertedIndex(int docId, List<Integer> frequency, List<List<Integer>> position) {
        this.docId = docId;
        this.frequency = frequency;
        this.position = position;
    }
}
