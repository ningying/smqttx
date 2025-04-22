package io.github.quickmsg.rule;

import io.github.quickmsg.common.rule.RuleDefinition;
import io.github.quickmsg.rule.node.DatabaseRuleNode;
import io.github.quickmsg.rule.node.EmptyNode;
import io.github.quickmsg.rule.node.PredicateRuleNode;
import io.github.quickmsg.rule.node.TransmitRuleNode;
import lombok.Getter;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.LinkedList;
import java.util.List;

/**
 * @author luxurong
 */
@Getter
public class RuleChain {

    private RuleChain() {
    }

    public final static RuleChain INSTANCE = new RuleChain();

    private LinkedList<RuleNode> ruleNodeList = new LinkedList<>();

    public void addRules(List<RuleDefinition> definitions) {
        RuleNode rootNode = this.parseNode(definitions.get(0));
        RuleNode preNode = rootNode;
        for (int i = 1; i < definitions.size(); i++) {
            RuleNode node = this.parseNode(definitions.get(i));
            preNode.setNextRuleNode(node);
            preNode = node;
        }
        ruleNodeList.addLast(rootNode);
    }


    private RuleNode parseNode(RuleDefinition definition) {
        switch (definition.getRuleType()) {
            case HTTP:
            case KAFKA:
            case ROCKET_MQ:
            case RABBIT_MQ:
            case MQTT:
                return new TransmitRuleNode(definition.getSourceId(), definition.getScript());
            case PREDICATE:
                return new PredicateRuleNode(definition.getScript());
            case DATA_BASE:
                return new DatabaseRuleNode(definition.getSourceId(), definition.getScript());
            default:
                return new EmptyNode();
        }
    }


    public Mono<Void> executeRule(ContextView contextView) {
        return Mono.fromRunnable(() -> ruleNodeList.forEach(ruleNode -> ruleNode.execute(contextView)));
    }

}
