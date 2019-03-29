package com.github.anicolaspp.ojai;

import com.mapr.db.rowcol.MutationImpl;
import com.mapr.db.rowcol.SerializedFamilyInfo;
import com.mapr.ojai.store.impl.Values;
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

public class InMemoryMutation extends MutationImpl {
    
    private List<MutationOp> ops = new ArrayList<>();
    
    private void newOp(String path, Value value, MutationOp.Type type) {
        MutationOp op = new MutationOp();
        
        op.setFieldPath(FieldPath.parseFrom(path));
        op.setOpValue(value);
        op.setType(type);

//        for (int i = 0; i < ops.size(); i++) {
//            if (ops.get(i).getFieldPath().asPathString().equals(op.getFieldPath().asPathString())) {
//                ops.set(i, op);
//
//                return;
//            }
//        }
        
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
        return super.set(path, v);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, Value v) {
        newOp(path.asPathString(), v, MutationOp.Type.SET);
        
        return super.set(path, v);
    }
    
    @Override
    public DocumentMutation set(String path, boolean b) {
        
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, boolean b) {
        newOp(path.asPathString(), new Values.BooleanValue(b), MutationOp.Type.SET);
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(String path, short s) {
        
        return super.set(path, s);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, short s) {
        newOp(path.asPathString(), new Values.ShortValue(s), MutationOp.Type.SET);
        
        return super.set(path, s);
    }
    
    @Override
    public DocumentMutation set(String path, byte b) {
        
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, byte b) {
        newOp(path.asPathString(), new Values.ByteValue(b), MutationOp.Type.SET);
        
        return super.set(path, b);
    }
    
    @Override
    public DocumentMutation set(String path, int i) {
        
        return super.set(path, i);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, int i) {
        newOp(path.asPathString(), new Values.IntValue(i), MutationOp.Type.SET);
        
        return super.set(path, i);
    }
    
    @Override
    public DocumentMutation set(String path, long l) {
        
        return super.set(path, l);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, long l) {
        newOp(path.asPathString(), new Values.LongValue(l), MutationOp.Type.SET);
        
        return super.set(path, l);
    }
    
    @Override
    public DocumentMutation set(String path, float f) {
        
        
        return super.set(path, f);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, float f) {
        newOp(path.asPathString(), new Values.FloatValue(f), MutationOp.Type.SET);
        
        return super.set(path, f);
    }
    
    @Override
    public DocumentMutation set(String path, double d) {
        
        return super.set(path, d);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, double d) {
        newOp(path.asPathString(), new Values.DoubleValue(d), MutationOp.Type.SET);
        
        return super.set(path, d);
    }
    
    @Override
    public MutationImpl set(String path, String value) {
        //newOp(path, new Values.StringValue(value), MutationOp.Type.SET);
        
        return super.set(path, value);
    }
    
    @Override
    public MutationImpl set(FieldPath path, String value) {
        newOp(path.asPathString(), new Values.StringValue(value), MutationOp.Type.SET);
        
        return super.set(path, value);
    }
    
    @Override
    public DocumentMutation set(String path, BigDecimal bd) {
        
        
        return super.set(path, bd);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, BigDecimal bd) {
        newOp(path.asPathString(), new Values.DoubleValue(bd.doubleValue()), MutationOp.Type.SET);
        
        return super.set(path, bd);
    }
    
    @Override
    public DocumentMutation set(String path, OTime t) {
        
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, OTime t) {
        newOp(path.asPathString(), new Values.TimeValue(t), MutationOp.Type.SET);
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(String path, OTimestamp t) {
        
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, OTimestamp t) {
        newOp(path.asPathString(), new Values.TimestampValue(t), MutationOp.Type.SET);
        
        return super.set(path, t);
    }
    
    @Override
    public DocumentMutation set(String path, ODate d) {
        
        
        return super.set(path, d);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, ODate d) {
        newOp(path.asPathString(), new Values.TimeValue(new OTime(d.toDate())), MutationOp.Type.SET);
        
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
        
        return super.set(path, intv);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, OInterval intv) {
        newOp(path.asPathString(), new Values.IntervalValue(intv), MutationOp.Type.SET);
        
        return super.set(path, intv);
    }
    
    @Override
    public DocumentMutation set(String path, ByteBuffer bb) {
        
        return super.set(path, bb);
    }
    
    @Override
    public DocumentMutation set(FieldPath path, ByteBuffer bb) {
        newOp(path.asPathString(), new Values.BinaryValue(bb), MutationOp.Type.SET);
        
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
        newOp(path.asPathString(), v, MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, v);
    }
    
    @Override
    public DocumentMutation setOrReplaceNull(String path) {
        return setNull(path);
    }
    
    @Override
    public DocumentMutation setOrReplaceNull(FieldPath path) {
        return setNull(path.asPathString());
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, boolean b) {
        
        
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, boolean b) {
        newOp(path.asPathString(), new Values.BooleanValue(b), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, short s) {
        
        
        return super.setOrReplace(path, s);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, short s) {
        newOp(path.asPathString(), new Values.ShortValue(s), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, s);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, byte b) {
        
        
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, byte b) {
        newOp(path.asPathString(), new Values.ByteValue(b), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, b);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, int i) {
        
        
        return super.setOrReplace(path, i);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, int i) {
        newOp(path.asPathString(), new Values.IntValue(i), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, i);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, long l) {
        
        
        return super.setOrReplace(path, l);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, long l) {
        newOp(path.asPathString(), new Values.LongValue(l), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, l);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, float f) {
        
        
        return super.setOrReplace(path, f);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, float f) {
        newOp(path.asPathString(), new Values.FloatValue(f), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, f);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, double d) {
        
        
        return super.setOrReplace(path, d);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, double d) {
        newOp(path.asPathString(), new Values.DoubleValue(d), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, d);
    }
    
    @Override
    public MutationImpl setOrReplace(String path, String value) {
        
        
        return super.setOrReplace(path, value);
    }
    
    @Override
    public MutationImpl setOrReplace(FieldPath path, String value) {
        newOp(path.asPathString(), new Values.StringValue(value), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, value);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, BigDecimal bd) {
        
        
        return super.setOrReplace(path, bd);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, BigDecimal bd) {
        newOp(path.asPathString(), new Values.DoubleValue(bd.doubleValue()), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, bd);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, OTime t) {
        
        
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, OTime t) {
        newOp(path.asPathString(), new Values.TimeValue(new OTime(t.toDate())), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, OTimestamp t) {
        
        
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, OTimestamp t) {
        newOp(path.asPathString(), new Values.TimestampValue(t), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, t);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, ODate d) {
        
        
        return super.setOrReplace(path, d);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, ODate d) {
        newOp(path.asPathString(), new Values.TimeValue(new OTime(d.toDate())), MutationOp.Type.SET_OR_REPLACE);
        
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
        newOp(path.asPathString(), new Values.IntervalValue(intv), MutationOp.Type.SET_OR_REPLACE);
        
        return super.setOrReplace(path, intv);
    }
    
    @Override
    public DocumentMutation setOrReplace(String path, ByteBuffer bb) {
        
        
        return super.setOrReplace(path, bb);
    }
    
    @Override
    public DocumentMutation setOrReplace(FieldPath path, ByteBuffer bb) {
        newOp(path.asPathString(), new Values.BinaryValue(bb), MutationOp.Type.SET_OR_REPLACE);
        
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
        
        newOp(path, new Values.ByteValue(inc), MutationOp.Type.INCREMENT);
        
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
        newOp(path, new Values.IntValue(inc), MutationOp.Type.INCREMENT);
        
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
        newOp(path, new Values.LongValue(inc), MutationOp.Type.INCREMENT);
    
        
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, float inc) {
        newOp(path, new Values.FloatValue(inc), MutationOp.Type.INCREMENT);
        
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, float inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, double inc) {
        newOp(path, new Values.DoubleValue(inc), MutationOp.Type.INCREMENT);
        
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(FieldPath path, double inc) {
        return super.increment(path, inc);
    }
    
    @Override
    public DocumentMutation increment(String path, BigDecimal inc) {
        newOp(path, new Values.DecimalValue(inc), MutationOp.Type.INCREMENT);
        
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
        newOp(path.asPathString(), new Values.NullValue(), MutationOp.Type.DELETE);
        
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
