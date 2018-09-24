package nebula.data.entity;

import static com.google.common.base.Preconditions.checkNotNull;

import nebula.data.impl.IDGenerator;
import nebula.data.impl.id.CurrentTimeIDGenerator;
import nebula.data.impl.id.ManualIDGenerator;
import nebula.data.impl.id.NativeIDGenerator;
import nebula.lang.Field;
import nebula.lang.Type;
import nebula.lang.TypeStandalone;

class IDGenerators {

    public static IDGenerator build(Type type) {
        Field keyField;
        String idGenerationStrategy;
        switch (type.getStandalone()) {
        case Transaction:
        case Relation:
            keyField = null;
            for (Field f : type.getFields()) {
                if (f.isKey()) {
                    if (f.getType().getStandalone() == TypeStandalone.Basic) {
                        keyField = f;
                    }
                }
            }
            checkNotNull(keyField);

            idGenerationStrategy = (String) keyField.getAttrs().get(Type.IDGenerationStrategy);

            if ("time".equals(idGenerationStrategy)) {
                return new CurrentTimeIDGenerator();
            } else if ("native".equals(idGenerationStrategy)) {
                return new NativeIDGenerator();
            } else if ("manual".equals(idGenerationStrategy)) {
                return new ManualIDGenerator();
            } else {
                throw new UnsupportedOperationException("not master,");
            }

        case Master:
            keyField = null;
            for (Field f : type.getFields()) {
                if (f.isKey()) {
                    if (f.getType().getStandalone() == TypeStandalone.Basic) {
                        keyField = f;
                    }
                }
            }
            checkNotNull(keyField);

            idGenerationStrategy = (String) keyField.getAttrs().get(Type.IDGenerationStrategy);

            if ("default".equals(idGenerationStrategy)) {
                return new CurrentTimeIDGenerator();
            } else if ("time".equals(idGenerationStrategy)) {
                return new CurrentTimeIDGenerator();
            } else if ("native".equals(idGenerationStrategy)) {
                return new NativeIDGenerator();
            } else if ("manual".equals(idGenerationStrategy)) {
                return new ManualIDGenerator();
            } else {
                throw new UnsupportedOperationException("not master,");
            }

        default:
            throw new UnsupportedOperationException("not master,");
        }
    }
}
