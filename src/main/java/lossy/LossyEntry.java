package lossy;


import java.util.Map;

/**
 * Created by Qiu on 11/10/15.
 */
public class LossyEntry{

    private String element;
    private Integer frequency;
    private Double delta;

    public LossyEntry(String element, Integer frequency, Double delta) {
        this.element = element;
        this.frequency = frequency;
        this.delta = delta;
    }

    public String getElement() {
        return element;
    }

    public void setElement(String element) {
        this.element = element;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }

    public Double getDelta() {
        return delta;
    }

    public void setDelta(Double delta) {
        this.delta = delta;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LossyEntry)) {
            return false;
        } else {
            return this.getElement().equals(((LossyEntry) obj).getElement());
        }

    }
}
