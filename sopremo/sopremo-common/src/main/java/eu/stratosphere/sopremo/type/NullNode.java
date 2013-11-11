package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.esotericsoftware.kryo.DefaultSerializer;

import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Singleton;
import eu.stratosphere.sopremo.SingletonSerializer;

/**
 * This node represents the value 'null'.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
@Singleton
@DefaultSerializer(NullNode.NullSerializer.class)
public class NullNode extends AbstractJsonNode implements IPrimitiveNode {

	private final static NullNode Instance = new NullNode();

	/**
	 * Initializes a NullNode. This constructor is needed for serialization and
	 * deserialization of NullNodes, please use NullNode.getInstance() to get the instance of NullNode.
	 */
	public NullNode() {
	}

	/**
	 * Returns the instance of NullNode.
	 * 
	 * @return the instance of NullNode
	 */
	public static NullNode getInstance() {
		return Instance;
	}

	@Override
	public void appendAsString(final Appendable sb) throws IOException {
		sb.append("null");
	}

	@Override
	public boolean equals(final Object o) {
		return o == Instance;
		// return o instanceof NullNode ? true : false;
	}

	@Override
	public NullNode canonicalize() {
		return Instance;
	}

	@Override
	public IJsonNode readResolve(final DataInput in) throws IOException {
		return Instance;
	}

	@Override
	public void write(final DataOutput out) throws IOException {
	}

	@Override
	public boolean isNull() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.NullNode;
	}

	@Override
	public NullNode clone() {
		return this;
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.checkForSameType(otherNode);
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return 0;
	}

	@Override
	public int hashCode() {
		return 37;
	}

	@Override
	public void clear() {
	}

	@Override
	public int getMaxNormalizedKeyLen() {
		return PactNull.getInstance().getMaxNormalizedKeyLen();
	}

	@Override
	public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		PactNull.getInstance().copyNormalizedKey(target, offset, len);
	}

	public static class NullSerializer extends SingletonSerializer {
		/**
		 * Initializes NullNode.NullSerializer.
		 */
		public NullSerializer() {
			super(NullNode.getInstance());
		}
	}
}
