package org.apache.zookeeper.server.util;

import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CircularBufferTest {
    private static Random random;
    private static List<Integer> usedValues;
    private int capacity;
    private CircularBuffer<Integer> circularBuffer;


    @BeforeAll
    public static void configureRandomGenerator() {
        CircularBufferTest.random = new Random(System.currentTimeMillis());
        CircularBufferTest.usedValues = new ArrayList<>();
    }

    @BeforeEach
    public void configureCircularBuffer() {
        this.capacity = CircularBufferTest.random.nextInt(1000);
        this.circularBuffer = new CircularBuffer<>(Integer.class, this.capacity);
    }

    @AfterEach
    public void clearEnviron() {
        CircularBufferTest.usedValues.clear();
    }

    @Test
    public void shouldThrowIllegalArgumentException() {
        int invalidCapacity = -100 + CircularBufferTest.random.nextInt(101);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new CircularBuffer<>(Integer.class, invalidCapacity));
    }

    @Test
    public void shouldThrowIllegalArgumentZeroCapacity() {
        Assertions.assertThrows(IllegalArgumentException.class, ()->new CircularBuffer<>(Integer.class, 0));
    }

    @Test
    public void shouldThrowException() {
        Assertions.assertThrows(Exception.class, () -> new CircularBuffer<>(null, 1));
    }

    @Test
    public void testWriteNull() {
        this.circularBuffer.write(null);
        Integer actual = this.circularBuffer.take();
        Assertions.assertNull(actual);
    }

    @Test
    public void testTake() {
        Assumptions.assumeTrue(this.capacity > 0);

        int oldestIndex = writeOnBufferAndGetOldest();
        int[] expectedValues = new int[this.circularBuffer.size()];
        int[] actualValues = new int[this.circularBuffer.size()];
        int k = 0;
        while(!this.circularBuffer.isEmpty()) {
            expectedValues[k] = CircularBufferTest.usedValues.get(oldestIndex);
            actualValues[k] = this.circularBuffer.take();
            oldestIndex++;
            k++;
        }
        Assertions.assertArrayEquals(expectedValues, actualValues);
    }

    @Test
    public void testPeek() {
        Assumptions.assumeTrue(this.capacity > 0);

        int oldestIndex = writeOnBufferAndGetOldest();
        int expectedValue = CircularBufferTest.usedValues.get(oldestIndex);
        int actualValue = this.circularBuffer.peek();
        Assertions.assertEquals(expectedValue, actualValue);

        actualValue = this.circularBuffer.peek();
        Assertions.assertEquals(expectedValue, actualValue);

        this.circularBuffer.write(CircularBufferTest.random.nextInt());
        expectedValue = CircularBufferTest.usedValues.get(oldestIndex + 1);
        actualValue = this.circularBuffer.peek();
        Assertions.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testSize() {
        int expectedValue = writeOnBufferAndGetExpectedSize();
        Assertions.assertEquals(expectedValue, this.circularBuffer.size());

        this.circularBuffer.take();
        expectedValue--;
        Assertions.assertEquals(expectedValue, this.circularBuffer.size());

    }

    @Test
    public void testIsEmpty() {
        Assertions.assertTrue(this.circularBuffer.isEmpty());

        int expectedSize = writeOnBufferAndGetExpectedSize();
        Assertions.assertFalse(this.circularBuffer.isEmpty());

        for(int i = 0; i < expectedSize; i++) {
            this.circularBuffer.take();
        }
        Assertions.assertTrue(this.circularBuffer.isEmpty());

        writeOnBufferAndGetExpectedSize();
        this.circularBuffer.reset();
        Assertions.assertTrue(this.circularBuffer.isEmpty());
    }

    @Test
    public void testIsFull() {
        Assertions.assertFalse(this.circularBuffer.isFull());

        while (CircularBufferTest.usedValues.size() < this.capacity) {
            writeOnBufferAndGetOldest();
        }
        Assertions.assertTrue(this.circularBuffer.isFull());

        this.circularBuffer.take();
        Assertions.assertFalse(this.circularBuffer.isFull());

        this.circularBuffer.reset();
        Assertions.assertFalse(this.circularBuffer.isFull());
    }

    @Test
    public void testPeekEmptyBuffer() {
        this.circularBuffer.write(1);
        this.circularBuffer.reset();
        Assertions.assertNull(this.circularBuffer.peek());
    }

    @Test
    public void testTakeEmptyBuffer() {
        this.circularBuffer.reset();
        Assertions.assertNull(this.circularBuffer.take());
    }

    private int writeOnBufferAndGetOldest() {
        int oldestIndex = 1;
        for(int i = 0; i < CircularBufferTest.random.nextInt(Integer.MAX_VALUE); i++) {
            Integer element = CircularBufferTest.random.nextInt();
            this.circularBuffer.write(element);
            CircularBufferTest.usedValues.add(element);
            if(i > this.capacity) {
                oldestIndex++;
            }
        }
        return oldestIndex;
    }

    private int writeOnBufferAndGetExpectedSize() {
        writeOnBufferAndGetOldest();
        return Math.min(CircularBufferTest.usedValues.size(), this.capacity);
    }

}
