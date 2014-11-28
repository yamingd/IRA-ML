package com.ml.ira;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public class ValueComparableMap<K extends Comparable<K>,V> extends TreeMap<K,V> {

    //A map for doing lookups on the keys for comparison so we don't get infinite loops
    private final Map<K, V> valueMap;

    public ValueComparableMap(final Ordering<? super V> partialValueOrdering) {
        this(partialValueOrdering, new HashMap<K,V>());
    }

    private ValueComparableMap(Ordering<? super V> partialValueOrdering,
                               HashMap<K, V> valueMap) {
        super(partialValueOrdering //Apply the value ordering
                .onResultOf(Functions.forMap(valueMap))
                .compound((Comparator<? super K>) Ordering.natural())); //On the result of getting the value for the key from the map
        this.valueMap = valueMap;
    }

    public V put(K k, V v) {
        if (valueMap.containsKey(k)){
            //remove the key in the sorted set before adding the key again
            remove(k);
        }
        valueMap.put(k,v); //To get "real" unsorted values for the comparator
        return super.put(k, v); //Put it in value order
    }

    @Override
    public void clear() {
        if (valueMap != null){
            valueMap.clear();
        }
        super.clear();
    }

    public static void main(String[] args){
        /*
        TreeMap<String, Integer> map = new ValueComparableMap<String, Integer>(Ordering.natural());
        map.put("a", 5);
        map.put("b", 1);
        map.put("c", 3);
        assertEquals("b",map.firstKey());
        assertEquals("a",map.lastKey());
        map.put("d",0);
        assertEquals("d",map.firstKey());
        //ensure it's still a map (by overwriting a key, but with a new value)
        map.put("d", 2);
        assertEquals("b", map.firstKey());
        //Ensure multiple values do not clobber keys
        map.put("e", 2);
        System.out.println(map.keySet());
        assertEquals(5, map.size());
        assertEquals(2, (int) map.get("e"));
        assertEquals(2, (int) map.get("d"));
        */
    }
}
