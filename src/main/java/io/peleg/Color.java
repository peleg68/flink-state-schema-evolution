package io.peleg;

public enum Color {
    BLUE(0),
    RED(1),
    PURPLE(2),
    GREEN(3),
    YELLOW(4);

    Color(int i) {
        this.i = i;
    }

    private final int i;
}
