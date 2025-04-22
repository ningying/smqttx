package io.github.quickmsg.rule.source;

import io.github.quickmsg.common.rule.source.SourceBean;
import io.github.quickmsg.common.rule.source.SourceDefinition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author luxurong
 */
public class SourceManager {

   private static final Map<String, SourceBean>  CACHE_BEANS = new ConcurrentHashMap<>();


   public static SourceBean getSourceBean(String sourceId){
       return CACHE_BEANS.get(sourceId);
   }

   public static void loadSource(SourceDefinition sourceDefinition){
      SourceBean.SOURCE_BEAN_LIST.forEach(sourceBean -> {
         if (sourceBean.support(sourceDefinition.getSource())) {
            if (sourceBean.bootstrap(sourceDefinition.getSourceAttributes())) {
               CACHE_BEANS.put(sourceDefinition.getSourceId(), sourceBean.copy());
            }
         }
      });
   }
}
