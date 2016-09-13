package plugins.flumeng.hdfs.source;

/**
 * @author chao.li
 *
 */
public class ChannelValue {
	private double click;
	private double impression;
	private double ctr;

	public ChannelValue() {
		super();
	}

	public double getClick() {
		return click;
	}

	public void setClick(double click) {
		this.click = click;
	}

	public double getImpression() {
		return impression;
	}

	public void setImpression(double impression) {
		this.impression = impression;
	}

	public double getCtr() {
		return ctr;
	}

	public void setCtr(double ctr) {
		this.ctr = ctr;
	}

}
