package com.mapr.db.impl;

import com.github.anicolaspp.ojai.DocumentProjector;
import com.mapr.db.rowcol.KeyValue;
import javafx.util.Pair;
import org.ojai.Document;
import org.ojai.Value;
import org.ojai.store.Connection;

import java.util.stream.Stream;

public class ConditionEvaluator {
    private ConditionNode condition;
    private Connection connection;

    public ConditionEvaluator(ConditionNode condition, Connection connection) {
        this.condition = condition;
        this.connection = connection;
    }

    public boolean evalOn(Document document) {
        return evalCondition(condition, document);
    }

    private boolean evalCondition(ConditionNode condition, Document document) {
        if (condition.isLeaf()) {
            ConditionLeaf leaf = (ConditionLeaf) condition;

            Document projected = new DocumentProjector(document, connection).projectPath(leaf.getField().asPathString());

            return getAllLeafValues(projected, "")
                    .filter(pair -> pair.getKey().equals(leaf.getField().asPathString().replace("[]", "")))
                    .map(Pair::getValue)
                    .anyMatch(value -> cmp(leaf.getValue(), value));
        } else {
            ConditionBlock block = (ConditionBlock) condition;

            if (block.getType() == ConditionBlock.BlockType.and) {
                return block.getChildren().stream().allMatch(cond -> evalCondition(cond, document));
            } else if (block.getType() == ConditionBlock.BlockType.or) {
                return block.getChildren().stream().anyMatch(cond -> evalCondition(cond, document));
            } else {
                return false;
            }
        }
    }

    private boolean cmp(KeyValue keyValue, Value value) {
        switch (value.getType()) {

            case NULL:
                return keyValue.getType().getCode() == Value.TYPE_CODE_NULL;
            case BOOLEAN:
                return keyValue.getBoolean() == value.getBoolean();
            case STRING:
                return keyValue.getString().equals(value.getString());
            case BYTE:
                return keyValue.getByte() == value.getByte();
            case SHORT:
                return keyValue.getShort() == value.getShort();
            case INT:
                return keyValue.getInt() == value.getInt();
            case LONG:
                return keyValue.getLong() == value.getLong();
            case FLOAT:
                return keyValue.getFloat() == value.getFloat();
            case DOUBLE:
                return keyValue.getDouble() == value.getDouble();
            case DECIMAL:
                return keyValue.getDecimal().equals(value.getDecimal());
            case DATE:
                return keyValue.getDate() == value.getDate();
            case TIME:
                return keyValue.getTime() == value.getTime();
            case TIMESTAMP:
                return keyValue.getTimestamp() == value.getTimestamp();
            case INTERVAL:
                return keyValue.getInterval() == value.getInterval();
            case BINARY:
                return keyValue.getBinary() == value.getBinary();
            case MAP:
                return keyValue.getMap() == value.getMap();
            case ARRAY:
                return keyValue.getList() == value.getList();
        }

        return false;
    }

    private String add(String parent, String key) {
        if (parent.isEmpty()) {
            return key;
        } else {
            return parent + "." + key;
        }
    }

    private Stream<Pair<String, Value>> getAllLeafValues(Document document, String parent) {
        return document.asMap().keySet()
                .stream()
                .flatMap(key -> {
                    Value value = document.getValue(key);

                    if (value.getType() == Value.Type.ARRAY) {
                        return value
                                .getList()
                                .stream()
                                .map(o -> connection.newDocument(o))
                                .flatMap(d -> getAllLeafValues(d, add(parent, key)));
                    } else if (value.getType() == Value.Type.MAP) {
                        return getAllLeafValues(connection.newDocument(value.getMap()), add(parent, key));
                    } else {
                        return Stream.of(new Pair<>(add(parent, key), value));
                    }
                });
    }

}
