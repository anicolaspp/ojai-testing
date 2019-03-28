package com.mapr.ojai.store.impl;

import com.mapr.db.rowcol.MutationImpl;
import com.mapr.db.rowcol.SerializedFamilyInfo;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;
import org.ojai.store.MutationOp;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class InMemoryMutation extends MutationImpl {
    
    private List<MutationOp> ops = new ArrayList<>();
    
    private void newOp(String path, Value value) {
        MutationOp op = new MutationOp();
        
        op.setFieldPath(FieldPath.parseFrom(path));
        op.setOpValue(value);
        
        ops.add(op);
    }
    
    private void nullOp(String path) {
        MutationOp op = new MutationOp();
    
        op.setFieldPath(FieldPath.parseFrom(path));
        op.setOpValue(null);
    
        ops.add(op);
    }
    
    @Override
    public DocumentMutation setNull(String path) {
        nullOp(path);
        
        return super.setNull(path);
    }
    
    @Override
    public DocumentMutation setNull(FieldPath path) {
        nullOp(path.asPathString());
        
        return super.setNull(path);
    }
    
    @Override
    public DocumentMutation set(String path, Value v) {
        newOp(path, v);
        
        return super.set(path, v);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, Value v) {
        newOp(path.asPathString(), v);
        
        return super.set(path, v);
    }
    
    @Override
    public DocumentMutation set(String path, boolean b) {
        newOp(path, new Values.BooleanValue(b));
        
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, boolean b) {
        newOp(path.asPathString(), new Values.BooleanValue(b));
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(String path, short s) {
        newOp(path, new Values.ShortValue(s));
        
        return super.set(path, s);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, short s) {
        newOp(path.asPathString(), new Values.ShortValue(s));
        
        return super.set(path, s);
    }
    
    @Override
    public DocumentMutation set(String path, byte b) {
        newOp(path, new Values.ByteValue(b));
        
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, byte b) {
        newOp(path.asPathString(), new Values.ByteValue(b));
        
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(String path, int i) {
        newOp(path, new Values.IntValue(i));
        
        return super.set(path, i);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, int i) {
        newOp(path.asPathString(), new Values.IntValue(i));
        
        return super.set(path, i);
    }
    
    @Override
    public DocumentMutation set(String path, long l) {
        newOp(path, new Values.LongValue(l));
        
        return super.set(path, l);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, long l) {
        newOp(path.asPathString(), new Values.LongValue(l));
        
        return super.set(path, l);
    }
    
    @Override
    public DocumentMutation set(String path, float f) {
        newOp(path, new Values.FloatValue(f));
        
        return super.set(path, f);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, float f) {
        newOp(path.asPathString(), new Values.FloatValue(f));
        
        return super.set(path, f);
    }
    
    @Override
    public DocumentMutation set(String path, double d) {
        newOp(path, new Values.DoubleValue(d));
        
        return super.set(path, d);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, double d) {
        newOp(path.asPathString(), new Values.DoubleValue(d));
        
        return super.set(path, d);
    }
    
    @Override
    public MutationImpl set(String path, String value) {
        newOp(path, new Values.StringValue(value));
        
        return super.set(path, value);
    }
    
    @Override
    public MutationImpl set(FieldPath path, String value) {
        newOp(path.asPathString(), new Values.StringValue(value));
        
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(String path, BigDecimal bd) {
        newOp(path, new Values.DoubleValue(bd.doubleValue()));
        
        return super.set(path, bd);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, BigDecimal bd) {
        newOp(path.asPathString(), new Values.DoubleValue(bd.doubleValue()));
        
        return super.set(path, bd);
    }
    
    @Override
    public DocumentMutation set(String path, OTime t) {
        newOp(path, new Values.TimeValue(t));
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, OTime t) {
        newOp(path.asPathString(), new Values.TimeValue(t));
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(String path, OTimestamp t) {
        newOp(path, new Values.TimestampValue(t));
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, OTimestamp t) {
        newOp(path.asPathString(), new Values.TimestampValue(t));
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(String path, ODate d) {
        newOp(path, new Values.TimeValue(new OTime(d.toDate())));
        
        return super.set(path, d);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, ODate d) {
        newOp(path.asPathString(), new Values.TimeValue(new OTime(d.toDate())));
        
        return super.set(path, d);
    }
    
    @Override
    public DocumentMutation set(String path, List<?> value) {
        
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, List<?> value) {
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(String path, OInterval intv) {
        newOp(path, new Values.IntervalValue(intv));
        
        return super.set(path, intv);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, OInterval intv) {
        newOp(path.asPathString(), new Values.IntervalValue(intv));
        
        return super.set(path, intv);
    }
    
    @Override
    public DocumentMutation set(String path, ByteBuffer bb) {
        newOp(path, new Values.BinaryValue(bb));
        
        return super.set(path, bb);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, ByteBuffer bb) {
        newOp(path.asPathString(), new Values.BinaryValue(bb));
        
        return super.set(path, bb);
    }
    
    @Override
    public DocumentMutation set(String path, Map<String, ?> value) {
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, Map<String, ?> value) {
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(String path, Document value) {
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, Document value) {
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, Value v) {
        return super.setOrReplace(path, v);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, Value v) {
        return super.setOrReplace(path, v);
    }
    
    @Override
    public DocumentMutation setOrReplaceNull(String path) {
        return super.setOrReplaceNull(path);
    }
    
    @Override
    public DocumentMutation setOrReplaceNull(FieldPath path) {
        return super.setOrReplaceNull(path);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, boolean b) {
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, boolean b) {
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, short s) {
        return super.setOrReplace(path, s);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, short s) {
        return super.setOrReplace(path, s);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, byte b) {
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, byte b) {
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, int i) {
        return super.setOrReplace(path, i);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, int i) {
        return super.setOrReplace(path, i);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, long l) {
        return super.setOrReplace(path, l);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, long l) {
        return super.setOrReplace(path, l);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, float f) {
        return super.setOrReplace(path, f);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, float f) {
        return super.setOrReplace(path, f);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, double d) {
        return super.setOrReplace(path, d);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, double d) {
        return super.setOrReplace(path, d);
    }
    
    @Override
    public MutationImpl setOrReplace(String path, String value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public MutationImpl setOrReplace(FieldPath path, String value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, BigDecimal bd) {
        return super.setOrReplace(path, bd);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, BigDecimal bd) {
        return super.setOrReplace(path, bd);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, OTime t) {
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, OTime t) {
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, OTimestamp t) {
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, OTimestamp t) {
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, ODate d) {
        return super.setOrReplace(path, d);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, ODate d) {
        return super.setOrReplace(path, d);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, List<?> value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, List<?> value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, OInterval intv) {
        return super.setOrReplace(path, intv);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, OInterval intv) {
        return super.setOrReplace(path, intv);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, ByteBuffer bb) {
        return super.setOrReplace(path, bb);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, ByteBuffer bb) {
        return super.setOrReplace(path, bb);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, Map<String, ?> value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, Map<String, ?> value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, Document value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, Document value) {
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation append(String path, List<?> value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(FieldPath path, List<?> value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(String path, String value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(FieldPath path, String value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(String path, byte[] value, int offset, int len) {
        return super.append(path, value, offset, len);
    }
    
    @Override
    public DocumentMutation append(FieldPath path, byte[] value, int offset, int len) {
        return super.append(path, value, offset, len);
    }
    
    @Override
    public DocumentMutation append(String path, byte[] value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(FieldPath path, byte[] value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(String path, ByteBuffer value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation append(FieldPath path, ByteBuffer value) {
        return super.append(path, value);
    }
    
    @Override
    public DocumentMutation merge(String path, Document value) {
        return super.merge(path, value);
    }
    
    @Override
    public DocumentMutation merge(FieldPath path, Document value) {
        return super.merge(path, value);
    }
    
    @Override
    public DocumentMutation merge(String path, Map<String, Object> value) {
        return super.merge(path, value);
    }
    
    @Override
    public DocumentMutation merge(FieldPath path, Map<String, Object> value) {
        return super.merge(path, value);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, byte inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, byte inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, short inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, short inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public MutationImpl increment(String path, int inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, int inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, long inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, long inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, float inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, float inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, double inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, double inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, BigDecimal inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, BigDecimal inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation delete(String path) {
        return super.delete(path);
    }
    
    @Override
    public DocumentMutation delete(FieldPath path) {
        return super.delete(path);
    }
    
    @Override
    public boolean needsReadOnServer() {
        return super.needsReadOnServer();
    }
    
    @Override
    public ByteBuffer rowcolSerialize() {
        return super.rowcolSerialize();
    }
    
    @Override
    public SerializedFamilyInfo[] rowcolSerialize(Map<FieldPath, Integer> jsonPathMap, boolean isBulkLoad) {
        return super.rowcolSerialize(jsonPathMap, isBulkLoad);
    }
    
    @Override
    public Map<Integer, List<String>> getFieldsNeedRead(Map<FieldPath, Integer> pathCFidMap) {
        return super.getFieldsNeedRead(pathCFidMap);
    }
    
    @Override
    public SerializedFamilyInfo[] rowcolSerialize(Map<FieldPath, Integer> jsonPathMap) {
        return super.rowcolSerialize(jsonPathMap);
    }
    
    @Override
    public Iterator<MutationOp> iterator() {
        return ops.iterator();
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, byte dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, byte dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, short dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, short dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, int dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, int dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, long dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, long dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, float dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, float dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, double dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, double dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(String path, BigDecimal dec) {
        return super.decrement(path, dec);
    }
    
    @Override
    public DocumentMutation decrement(FieldPath path, BigDecimal dec) {
        return super.decrement(path, dec);
    }
}
