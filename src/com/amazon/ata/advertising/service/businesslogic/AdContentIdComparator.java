package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.model.AdvertisementContent;

import java.util.Comparator;
import java.util.HashMap;

public class AdContentIdComparator implements Comparator<AdvertisementContent> {
    private final HashMap<String, Integer> indexMap;

    public AdContentIdComparator(HashMap<String, Integer> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public int compare(AdvertisementContent left, AdvertisementContent right) {
        Integer leftIndex = indexMap.get(left.getContentId());
        Integer rightIndex = indexMap.get(right.getContentId());
        if (leftIndex == null) {
            return -1;
        }
        if (rightIndex == null) {
            return 1;
        }

        return Integer.compare(leftIndex, rightIndex);
    }
}
