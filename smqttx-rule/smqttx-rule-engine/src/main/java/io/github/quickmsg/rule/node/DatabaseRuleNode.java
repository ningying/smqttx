package io.github.quickmsg.rule.node;

import io.github.quickmsg.common.message.Message;
import io.github.quickmsg.common.rule.source.Source;
import io.github.quickmsg.rule.RuleNode;
import io.github.quickmsg.rule.source.SourceManager;
import reactor.util.context.ContextView;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author luxurong
 */
public class DatabaseRuleNode implements RuleNode {

    private final String script;

    private RuleNode ruleNode;

    private String sourceId;

    public DatabaseRuleNode(String sourceId, String script) {
        this.script = script;
        this.sourceId = sourceId;
    }

    @Override
    public RuleNode getNextRuleNode() {
        return this.ruleNode;
    }


    @Override
    public void setNextRuleNode(RuleNode ruleNode) {
        this.ruleNode = ruleNode;
    }

    @Override
    public void execute(ContextView contextView) {
        Map<String,Object> message = contextView.get(Map.class);
        if (script != null) {
            Optional.ofNullable(SourceManager.getSourceBean(sourceId))
                    .ifPresent(sourceBean -> sourceBean.transmit(triggerTemplate(script, context -> message.forEach(context::set))));
        }
        executeNext(contextView);
    }
}



