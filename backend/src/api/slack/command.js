"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("../../conf/index");
const slackCommandService_1 = __importDefault(require("../../services/slackCommandService"));
exports.default = async (req, res) => {
    // verify request
    if (req.body.token === index_1.SLACK_CONFIG.appToken &&
        req.body.team_id === index_1.SLACK_CONFIG.teamId &&
        req.body.api_app_id === index_1.SLACK_CONFIG.appId) {
        const command = req.body.command;
        const params = req.body.text;
        const username = req.body.user_name;
        const userId = req.body.user_id;
        try {
            const result = await new slackCommandService_1.default(req).processCommand(command, params, username, userId);
            res.setHeader('content-type', 'application/json');
            res.send(result);
        }
        catch (err) {
            req.log.error(err, 'Error processing slack command!');
            res.setHeader('content-type', 'application/json');
            const blocks = [
                {
                    type: 'section',
                    text: {
                        type: 'mrkdwn',
                        text: '*Error processing slack command!*',
                    },
                },
                {
                    type: 'section',
                    text: {
                        type: 'mrkdwn',
                        text: `\`\`\`${JSON.stringify(err, undefined, 2).substring(0, 2500)}\`\`\``,
                    },
                },
            ];
            res.send({
                blocks,
            });
        }
    }
    else {
        req.log.warn({ data: JSON.stringify(req.body) }, 'Received unverified slack command!');
        res.sendStatus(200);
    }
};
//# sourceMappingURL=command.js.map