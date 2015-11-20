package lossy;


import java.io.Serializable;

/**
 * Created by Qiu on 11/10/15.
 */
public class LossyEntry implements Comparable<LossyEntry>, Serializable{

    private String element;
    private Integer frequency;
    private Integer delta;

    public LossyEntry(String element, Integer frequency, Integer delta) {
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

    public Integer getDelta() {
        return delta;
    }

    public void setDelta(Integer delta) {
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

    @Override
    public int compareTo(LossyEntry o) {
        return this.getFrequency().compareTo(o.getFrequency());
    }
}
