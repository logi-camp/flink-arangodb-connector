package top.logicamp.arangodb_flink_connector.serde;

import com.arangodb.entity.BaseDocument;

import java.io.Serializable;

/** DocumentSerializer serialize POJOs or other Java objects into {@link BaseDocument}. */
public interface DocumentSerializer<T> extends Serializable {

    /**
     * Serialize input Java objects into {@link BaseDocument}.
     *
     * @param object The input object.
     * @return The serialized {@link BaseDocument}.
     */
    BaseDocument serialize(T object);
}
