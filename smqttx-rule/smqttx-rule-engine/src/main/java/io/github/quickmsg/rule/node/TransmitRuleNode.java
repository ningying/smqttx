package io.github.quickmsg.rule.node;

import io.github.quickmsg.common.rule.source.Source;
import io.github.quickmsg.common.utils.JacksonUtil;
import io.github.quickmsg.rule.RuleNode;
import io.github.quickmsg.rule.source.SourceManager;
import reactor.util.context.ContextView;

import java.util.Map;

/**
 * 转发节点
 *
 * @author luxurong
 */
public class TransmitRuleNode implements RuleNode {

    private final String script;

    private RuleNode ruleNode;

    private String sourceId;


    public TransmitRuleNode(String sourceId, String script) {
        this.script = script;
        this.sourceId = sourceId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(ContextView contextView) {
        Object param;
        if (script != null) {
            param = triggerScript(script, context -> {
                Map<String, Object> message = contextView.get(Map.class);
                message.forEach(context::set);
            });
        } else {
            param = contextView.get(Map.class);
        }
        SourceManager.getSourceBean(sourceId).transmit(param);
        executeNext(contextView);
    }

    @Override
    public RuleNode getNextRuleNode() {
        return this.ruleNode;
    }


    @Override
    public void setNextRuleNode(RuleNode ruleNode) {
        this.ruleNode = ruleNode;
    }

}
