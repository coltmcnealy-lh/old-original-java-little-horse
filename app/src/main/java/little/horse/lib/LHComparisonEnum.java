package little.horse.lib;

public enum LHComparisonEnum {
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LESS_THAN_EQ("<="),
    GRREATER_THAN_EQ(">="),
    EQUAL("=="),
    IN("IN"),
    NOT_IN("NOT_IN");
    
    String message;
    private LHComparisonEnum(String message) {
        this.message = message;
    }

    public String toString() {
        return message;
    }
}
