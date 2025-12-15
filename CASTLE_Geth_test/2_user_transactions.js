const { ethers } = require('ethers');
const fs = require('fs');

// 配置參數
const CONFIG = {
    NUM_TRANSACTIONS: 100000,
    GAS_PRICE_GWEI: 20,
    GAS_LIMIT: 21000,
    MIN_TX_AMOUNT: 0.0001,
    MAX_TX_AMOUNT: 0.001,
    PROGRESS_LOG_INTERVAL: 50,
    ACCOUNTS_FILE: './accounts.json'
};

console.log('🔧 交易配置:', CONFIG);

class UserTransactions {
    constructor() {
        this.provider = new ethers.JsonRpcProvider('http://127.0.0.1:8545');
        this.users = [];
        this.userNonces = new Map();
        this.successCount = 0;
        this.errorCount = 0;
    }

    async checkAndRecoverStuckBlocks() {
        const currentBlock = await this.provider.getBlockNumber();
        await new Promise(r => setTimeout(r, 2000));
        const newBlock = await this.provider.getBlockNumber();

        if (currentBlock === newBlock) {
            console.log(`⚠️  偵測到區塊停止在 ${currentBlock}，嘗試觸發挖礦...`);

            try {
                // 使用第一個用戶發送觸發交易
                const wallet = this.users[0];
                const tx = await wallet.sendTransaction({
                    to: wallet.address,
                    value: 1, // 1 wei
                    gasLimit: 21000
                });
                console.log(`⚙️ 已發送觸發交易: ${tx.hash}`);

                await new Promise(r => setTimeout(r, 3000));
                const finalBlock = await this.provider.getBlockNumber();
                console.log(`✅ 區塊從 ${currentBlock} 恢復到 ${finalBlock}`);

            } catch (error) {
                console.log(`❌ 無法觸發挖礦: ${error.message}`);
                throw new Error('區塊鏈停止，請手動重啟 Geth');
            }
        }
    }

    async loadAccounts() {
        console.log(`📂 載入帳號資訊從 ${CONFIG.ACCOUNTS_FILE}...`);

        if (!fs.existsSync(CONFIG.ACCOUNTS_FILE)) {
            throw new Error(`找不到帳號文件: ${CONFIG.ACCOUNTS_FILE}，請先執行 1_setup_accounts.js`);
        }

        const data = JSON.parse(fs.readFileSync(CONFIG.ACCOUNTS_FILE, 'utf8'));

        console.log(`載入配置: 用戶數量 = ${data.config.NUM_USERS}, 每人資金 = ${data.config.FUND_AMOUNT_ETH} ETH`);
        console.log(`帳號創建時間: ${data.timestamp}`);

        // 將用戶資訊轉換為 Wallet 對象
        for (const user of data.users) {
            const wallet = new ethers.Wallet(user.privateKey, this.provider);
            this.users.push(wallet);
            // 不在這裡設置 nonce，稍後從鏈上讀取
        }

        console.log(`✅ 已載入 ${this.users.length} 個用戶`);
    }

    async initialize() {
        console.log('🚀 初始化交易環境...');

        // 檢查 Geth 連接
        try {
            const network = await this.provider.getNetwork();
            const blockNumber = await this.provider.getBlockNumber();
            console.log(`✅ 已連接到 Geth (ChainID: ${network.chainId}, 區塊: ${blockNumber})`);
        } catch (error) {
            console.error('❌ 無法連接到 Geth，請確認 Geth 正在運行於 http://127.0.0.1:8545');
            throw error;
        }

        await this.loadAccounts();
    }

    async sendUserTransaction(fromIndex, toIndex, amount) {
        const sender = this.users[fromIndex];
        const receiver = this.users[toIndex];

        // 如果沒有本地記錄的 nonce，從鏈上讀取
        let currentNonce = this.userNonces.get(sender.address);
        if (currentNonce === undefined) {
            currentNonce = await this.provider.getTransactionCount(sender.address);
            this.userNonces.set(sender.address, currentNonce);
        }

        const tx = await sender.sendTransaction({
            to: receiver.address,
            value: ethers.parseEther(amount.toString()),
            gasLimit: CONFIG.GAS_LIMIT,
            gasPrice: ethers.parseUnits(CONFIG.GAS_PRICE_GWEI.toString(), 'gwei'),
            nonce: currentNonce
        });

        this.userNonces.set(sender.address, currentNonce + 1);

        // 等待交易確認
        const receipt = await Promise.race([
            tx.wait(1),
            new Promise((_, reject) =>
                setTimeout(() => reject(new Error('交易確認超時')), 60000)
            )
        ]);

        return { tx, receipt };
    }

    async runTransactions() {
        console.log(`🔥 執行 ${CONFIG.NUM_TRANSACTIONS} 筆用戶間交易...`);
        const startTime = Date.now();
        const numUsers = this.users.length;

        for (let i = 0; i < CONFIG.NUM_TRANSACTIONS; i++) {
            try {
                // 隨機選擇發送者和接收者
                const fromIndex = Math.floor(Math.random() * numUsers);
                let toIndex = Math.floor(Math.random() * numUsers);
                while (toIndex === fromIndex) {
                    toIndex = Math.floor(Math.random() * numUsers);
                }

                // 隨機金額
                const amount = (
                    Math.random() * (CONFIG.MAX_TX_AMOUNT - CONFIG.MIN_TX_AMOUNT) +
                    CONFIG.MIN_TX_AMOUNT
                ).toFixed(6);

                await this.sendUserTransaction(fromIndex, toIndex, amount);
                this.successCount++;

                if ((i + 1) % CONFIG.PROGRESS_LOG_INTERVAL === 0) {
                    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                    console.log(`進度: ${i + 1}/${CONFIG.NUM_TRANSACTIONS} (${elapsed}s, 成功: ${this.successCount}, 失敗: ${this.errorCount})`);
                }

            } catch (error) {
                this.errorCount++;
                console.error(`交易 ${i + 1} 失敗:`, error.message);

                // 如果是超時錯誤，檢查是否區塊停止
                if (error.message.includes('超時')) {
                    console.log('🔄 交易超時，檢查區塊狀態...');
                    await this.checkAndRecoverStuckBlocks();
                }
            }
        }

        const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`✅ 用戶交易完成！耗時: ${totalTime}s`);
    }

    async run() {
        try {
            const testStartTime = Date.now();

            await this.initialize();
            await this.runTransactions();

            const totalTime = ((Date.now() - testStartTime) / 1000).toFixed(1);
            console.log('\n🎉 交易測試完成！');
            console.log(`總耗時: ${totalTime}s`);
            console.log(`成功交易: ${this.successCount}/${CONFIG.NUM_TRANSACTIONS}`);
            console.log(`失敗交易: ${this.errorCount}/${CONFIG.NUM_TRANSACTIONS}`);

        } catch (error) {
            console.error('❌ 測試失敗:', error);
            process.exit(1);
        }
    }
}

// 執行交易測試
new UserTransactions().run().then(() => process.exit(0)).catch(e => {
    console.error('程式錯誤:', e);
    process.exit(1);
});
