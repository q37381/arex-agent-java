package io.arex.inst.dubbo.common;

import io.arex.agent.bootstrap.model.Mocker;
import io.arex.agent.bootstrap.util.ArrayUtils;
import io.arex.agent.bootstrap.util.StringUtil;
import io.arex.inst.runtime.config.Config;
import io.arex.inst.runtime.context.ContextManager;
import io.arex.inst.runtime.log.LogManager;
import io.arex.inst.runtime.model.ArexConstants;
import io.arex.inst.runtime.serializer.Serializer;
import io.arex.inst.runtime.util.MockUtils;
import io.arex.inst.runtime.util.TypeUtil;

import java.lang.reflect.Array;
import java.util.*;

public abstract class AbstractAdapter {
    /**
     * method name of the dubbo generic invoke
     */
    public static final String GENERIC_INVOKE_METHOD_NAME = "$invoke";

    private static final Set<String> FILTER_KEY_SET = new HashSet<>();

    static {
        FILTER_KEY_SET.add("schema");
        FILTER_KEY_SET.add("class");
    }

    protected void doExecute(Object result, Mocker mocker) {
        if (result != null) {
            mocker.getTargetResponse().setBody(Serializer.serialize(result));
            // maybe throwable
            mocker.getTargetResponse().setType(TypeUtil.getName(result));
        }
        if (ContextManager.needReplay()) {
            MockUtils.replayMocker(mocker);
        } else {
            MockUtils.recordMocker(mocker);
        }
    }

    /**
     * Standardize the response. When Dubbo generic calls, the response contains schema/class, which needs to be removed
     */
    protected Object normalizeResponse(Object response, boolean isGeneric) {
        if (!isGeneric) {
            return response;
        }
        if (!(response instanceof Map)) {
            return response;
        }

        Map<String, Object> responseMap = (Map<String, Object>) response;
        for (String filterKey : FILTER_KEY_SET) {
            responseMap.remove(filterKey);
        }
        for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
            Object clearedValue = normalizeResponse(entry.getValue(), isGeneric);
            responseMap.put(entry.getKey(), clearedValue);
        }

        return responseMap;
    }

    public abstract String getOperationName();

    /**
     * Whether the invocation is a raw generic $invoke call: the provider interface itself implements
     * GenericService, so Dubbo's GenericFilter deliberately keeps the raw triplet
     * [methodName, parameterTypes, arguments] as the invocation arguments and only the literal
     * "$invoke" method can be dispatched by the provider
     */
    protected boolean isRawGenericInvoke() {
        if (!GENERIC_INVOKE_METHOD_NAME.equals(getMethodName())) {
            return false;
        }
        Object[] arguments = getArguments();
        return arguments != null && arguments.length == 3
                && arguments[0] instanceof String
                && arguments[1] instanceof String[]
                && arguments[2] instanceof Object[];
    }

    protected abstract String getMethodName();

    public String getServiceOperation() {
        return getPath() + "." + getOperationName();
    }

    public String getRequest() {
        String originalRequest = getAttachment(ArexConstants.ORIGINAL_REQUEST);
        if (StringUtil.isNotEmpty(originalRequest)) {
            return originalRequest;
        }

        return serializeArguments(getRealArguments(), ArexConstants.JACKSON_REQUEST_SERIALIZER);
    }

    /**
     * The real arguments of the business call. For a raw generic $invoke invocation the triplet is
     * unwrapped, so that the recorded request keeps the same form as the consumer-side call, which is
     * exactly what replay rebuilds through $invoke(methodName, parameterTypes, arguments)
     */
    public Object[] getRealArguments() {
        if (isRawGenericInvoke()) {
            return (Object[]) getArguments()[2];
        }
        return getArguments();
    }

    protected String serializeArguments(Object[] arguments, String serializer) {
        if (StringUtil.isEmpty(Config.get().getString(DubboConstants.EXCLUDE_MAP_KEYS_CONFIG))) {
            String result = Serializer.serialize(arguments, serializer);
            if (result != null || ArrayUtils.isEmpty(arguments)) {
                return result;
            }
        }
        if (ArrayUtils.isEmpty(arguments)) {
            return Serializer.serialize(arguments, serializer);
        }
        Object[] filteredArguments = new Object[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            filteredArguments[i] = filterUnSerializable(arguments[i]);
        }
        return Serializer.serialize(filteredArguments, serializer);
    }

    private Object filterUnSerializable(Object value) {
        if (value == null) {
            return null;
        }
        // when the exclude config is present, containers are always rebuilt
        // so that the configured keys can be dropped at any depth
        if (value instanceof Map) {
            return filterMap((Map<?, ?>) value);
        }
        if (value instanceof Collection) {
            return filterCollection((Collection<?>) value);
        }
        if (isObjectArray(value)) {
            return filterArray(value);
        }
        return value;
    }

    private Map<Object, Object> filterMap(Map<?, ?> map) {
        Map<Object, Object> filteredMap = new LinkedHashMap<>();
        Set<String> excludeMapKeys = getExcludeMapKeys();
        Set<String> excludePrefixMapKeys = getExcludePrefixMapKeys();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (excludeMapKeys.contains(key)) {
                continue;
            }
            boolean exclude = false;
            for (String excludePrefixMapKey : excludePrefixMapKeys) {
                if (key.startsWith(excludePrefixMapKey)) {
                    exclude = true;
                    break;
                }
            }
            if (!exclude) {
                filteredMap.put(entry.getKey(), entry.getValue());
            }
        }
        return filteredMap;
    }

    private Collection<Object> filterCollection(Collection<?> collection) {
        Collection<Object> filtered = collection instanceof Set ? new LinkedHashSet<Object>() : new ArrayList<Object>();
        for (Object element : collection) {
            filtered.add(filterUnSerializable(element));
        }
        return filtered;
    }

    private Object[] filterArray(Object array) {
        Object[] filtered = new Object[Array.getLength(array)];
        for (int i = 0; i < filtered.length; i++) {
            filtered[i] = filterUnSerializable(Array.get(array, i));
        }
        return filtered;
    }

    private boolean isObjectArray(Object value) {
        return value.getClass().isArray() && !value.getClass().getComponentType().isPrimitive();
    }

    private boolean canSerialize(Object value, String serializer) {
        try {
            Serializer.serializeWithException(value, serializer);
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }

    private static volatile String excludeMapKeysConfig;
    private static volatile Set<String> excludeMapKeys = Collections.emptySet();
    private static volatile Set<String> excludePrefixMapKeys = Collections.emptySet();

    /**
     * parse the exclude map keys config, cached until the config value changes
     */
    private void reloadExcludeMapKeysIfModified() {
        String configValue = Config.get().getString(DubboConstants.EXCLUDE_MAP_KEYS_CONFIG);
        if (configValue == null) {
            // fallback: JVM arg / config file, then hardcoded default
            configValue = System.getProperty(DubboConstants.EXCLUDE_MAP_KEYS_CONFIG,
                    DubboConstants.EXCLUDE_MAP_KEYS_CONFIG);
        }
        if (configValue == null || configValue.equals(excludeMapKeysConfig)) {
            return;
        }
        if (configValue.isEmpty()) {
            excludeMapKeysConfig = "";
            excludeMapKeys = Collections.emptySet();
            excludePrefixMapKeys = Collections.emptySet();
            return;
        }
        Set<String> keys = new HashSet<>();
        Set<String> prefixKeys = new HashSet<>();
        for (String key : StringUtil.split(configValue, ',')) {
            String trimmed = key.trim();
            if (StringUtil.isNotEmpty(trimmed)) {
                if (trimmed.endsWith("*")) {
                    prefixKeys.add(trimmed.substring(0, trimmed.length() - 1));
                } else {
                    keys.add(trimmed);
                }
            }
        }
        excludeMapKeysConfig = configValue;
        excludeMapKeys = keys;
        excludePrefixMapKeys = prefixKeys;
    }

    private Set<String> getExcludeMapKeys() {
        reloadExcludeMapKeysIfModified();
        return excludeMapKeys;
    }

    private Set<String> getExcludePrefixMapKeys() {
        reloadExcludeMapKeysIfModified();
        return excludePrefixMapKeys;
    }

    /**
     * for dubbo generic invoke
     */
    public String getRequestParamType() {
        return ArrayUtils.toString(getParameterTypes(), obj -> ((Class<?>)obj).getName());
    }

    /**
     * arex record request type, used when the dubbo generic invoke serialize cannot be resolved
     */
    public String getRecordRequestType() {
        return ArrayUtils.toString(getArguments(), TypeUtil::getName);
    }
    public abstract String getProtocol();
    public String getExcludeMockTemplate() {
        return getAttachment(ArexConstants.HEADER_EXCLUDE_MOCK);
    }

    public String getCaseId() {
        return getAttachment(ArexConstants.RECORD_ID);
    }
    public boolean forceRecord() {
        return Boolean.parseBoolean(getAttachment(ArexConstants.FORCE_RECORD, ArexConstants.HEADER_X_PREFIX));
    }

    public boolean replayWarmUp() {
        return Boolean.parseBoolean(getAttachment(ArexConstants.REPLAY_WARM_UP));
    }

    public String getGeneric() {
        return getValByKey(DubboConstants.KEY_GENERIC);
    }

    public String getConfigVersion() {
        return getAttachment(ArexConstants.CONFIG_VERSION);
    }

    public Map<String, String> getRequestHeaders() {
        Map<String, String> headerMap = new HashMap<>(getAttachments());
        // arex puts the serialized original request into an attachment to
        // prevent the application from modifying the request, which needs to be removed when get all attachments
        headerMap.remove(ArexConstants.ORIGINAL_REQUEST);
        headerMap.put(DubboConstants.KEY_PROTOCOL, getProtocol());
        headerMap.put(DubboConstants.KEY_GROUP, getValByKey(DubboConstants.KEY_GROUP));
        headerMap.put(DubboConstants.KEY_VERSION, getValByKey(DubboConstants.KEY_VERSION));
        return headerMap;
    }

    public String getPath() {
        return StringUtil.defaultIfEmpty(getAttachment("path"), getServiceName());
    }

    /**
     * First get the value from the attachment, if it is empty, get the value from the parameter
     */
    protected String getValByKey(String key) {
        String value = getAttachment(key);
        if (StringUtil.isNotEmpty(value)) {
            return value;
        }
        return getParameter(key);
    }

    protected abstract String getAttachment(String key);

    protected abstract Map<String, String> getAttachments();

    protected abstract String getParameter(String key);

    protected String getAttachment(String key, String prefix) {
        String value = getAttachment(key);
        if (StringUtil.isNotEmpty(value) || StringUtil.isEmpty(prefix)) {
            return value;
        }
        return getAttachment(prefix + key);
    }

    public abstract String getServiceName();

    protected abstract Object[] getArguments();

    protected abstract Class<?>[] getParameterTypes();
}
