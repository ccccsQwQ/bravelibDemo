<?php

namespace Modules\Gift\Http\Repositories;

use Exception;
use Illuminate\Support\Facades\DB;
use JetBrains\PhpStorm\Pure;
use Jobs\Demo;
use Throwable;

class GiftRepository
{
    #[Pure] public static function Factory(): GiftRepository
    {
        return new self();
    }

    /**
     * 收到礼物的人，将礼物加入礼物墙
     */
    public function addGiftWall($uid, $gift_id, $gift_number = 1): void
    {
        if (GiftWall::query()->where(['uid' => $uid, 'gift_id' => $gift_id])->exists()) {
            GiftWall::query()->where(['uid' => $uid, 'gift_id' => $gift_id])->increment('gift_number', $gift_number);
        } else {
            GiftWall::query()->create([
                'gift_number' => $gift_number, 'uid' => $uid, 'gift_id' => $gift_id
            ]);
        }
    }

    /**
     * 赠送幸运礼物(按组赠送礼物)
     * 并发场景下崩溃和卡顿原因：
     * 假设出现A送B,同时B送A的场景
     * A请求执行到查询coin并加锁
     * 此时B请求也恰好执行到查询coin并加锁
     * 接着A请求执行到增加B用户coin，此时B请求已经把数据锁住，因此A请求在此停顿等待B请求解锁
     * 此时B请求执行到增加A用户coin，同上，B请求需要等待A请求解锁
     * 这情况下，A等B解锁，B等A解锁，形成死锁，在并发场景下这个情况可能会出现多次，导致资源紧张直到崩溃
     * 解决思路：避免死锁，延迟update操作，在业务上做调整，通知用户coin到账可能会有延迟
     * @throws Throwable
     */
    public function doGiveGroupGift($uid, $to_uid, $giftInfo, $scene, $room_id, $number_group, $eachUserGiftCoin, $giftUnitCoin)
    {
        try {
            return DB::transaction(function () use ($room_id, $uid, $to_uid, $giftInfo, $scene, $number_group, $eachUserGiftCoin, $giftUnitCoin) {
                // 解决办法三：
                // coin字段设置为非负，此处不需要上锁只需要判断余额是否充足，当并发场景导致验证异常执时，
                // coin不足导致coin值为负数时数据库抛出异常，在下方catch处根据业务做对应处理
                // 解决办法四（不建议）：
                // 把uid和coin读到redis
                // 校验coin通过读redis完成，update执行时写入数据库后同步到redis
                // redis上锁不会影响到后续的数据库update
                // 但是会出现高并发场景下的数据库和缓存数据不一致问题，可以通过缓存双删来缓解，没法根除
                # 赠送礼物：为防止高并发跳过校验，加锁
                $userCoinBalance = User::query()->where(['id' => $uid])->lockForUpdate()->value('coin');
                if ($userCoinBalance < $eachUserGiftCoin) {
                    throw new \Illuminate\Database\QueryException('余额不足，扣款失败', [], new \PDOException());
                }
                User::query()->where('id', $uid)->decrement('coin', $eachUserGiftCoin);

                # 记录礼物赠送记录
                $giftBillInfo = [
                    'uid'          => $uid,
                    'to_uid'       => $to_uid,
                    'gift_id'      => $giftInfo['id'],
                    'gift_name'    => $giftInfo['name'],
                    'gift_number'  => 1, //
                    'number_group' => $number_group, //礼物组数
                    'gift_coin'    => $eachUserGiftCoin,
                    'gift_money'   => $this->coin2money($eachUserGiftCoin),
                    'room_id'      => $room_id,
                ];

                $bill_id = GiftBill::query()->insertGetId($giftBillInfo);

                # 上礼物墙
                $this->addGiftWall($to_uid, $giftInfo['id']);

                // 记录收礼人返币帐单
                $multipleNum = 100;//通过一定算法计算该礼物会返回给赠送者N倍的礼物价值;
                $backCoin    = bcmul($multipleNum, $giftUnitCoin, 8);
                if ($backCoin > 0) {
                    // 解决办法一：
                    // 此处不直接处理coin增加，可以选择通过队列增加coin
                    // UserCoinQueueService::IncreCoinByUid($to_uid, $backCoin);
                    // 解决办法二：
                    // 此处不直接处理coin增加，通过定时任务扫bill表，读间隔时间段内新增的数据后更新User表的coin
                    User::query()->where('id', $to_uid)->increment('coin', $backCoin);
                    $userCoinBalance = floatval($userCoinBalance) + floatval($backCoin);
                    $bill            = [
                        'title'   => '幸运礼物奖励',
                        'uid'     => $to_uid,
                        'number'  => $backCoin,
                        'link_id' => $bill_id,
                        'balance' => $userCoinBalance,
                        'mark'    => '',
                        'status'  => 1,
                        'room_id' => $room_id
                    ];
                    UserBill::query()->insertGetId($bill);
                }

                # 更新 房间排行榜 排行榜数据
                RankingDataRepository::Factory()->giveGroupGiftUpdateRankings($room_id, $uid, $to_uid, $giftInfo, $eachUserGiftCoin);

                # 更新用户在房间内的贡献币数和收益币数
                RoomMemberRepository::Factory()->updateUserRoomIncomeAndExpend($room_id, $uid, $to_uid, $eachUserGiftCoin);

                # 更新房间总消费币数和金额
                if ($room_id) {
                    VcrRoom::query()->where('id', $room_id)->increment('spend_coin', $giftBillInfo['gift_coin']);
                    VcrRoom::query()->where('id', $room_id)->increment('spend_money', $giftBillInfo['gift_money']);
                }

                # 记录礼物收益分配
                $this->giftGroupIncomeDistribution($eachUserGiftCoin, $room_id, $bill_id, $to_uid);

                # 更新送礼物人魅力值
                Demo::dispatch([
                    "msg" => '更新送礼物人魅力值'
                ]);

                # 赠送礼物，等级加分
                Demo::dispatch([
                    "msg" => '送礼物等级加分'
                ]);

                # 礼物通知
                Demo::dispatch([
                    "msg" => '礼物通知XXX送AAA礼物'
                ]);
                # 推送房间公告通知
                if ($multipleNum >= 500) {
                    Demo::dispatch([
                        "msg" => '飘屏 喜中' . $multipleNum . '倍'
                    ]);
                }

                if ($room_id && $multipleNum > 0) {
                    Demo::dispatch([
                        "msg" => ' 喜中' . $multipleNum . '倍'
                    ])->delay(now()->addSeconds(1));
                }
                return true;
            });
        } catch (Throwable $e) {
            dp($e->getMessage(), $e->getTraceAsString());
            return false;
        }
    }

    /**
     * 分组礼物收益分成操作
     * User:ytx
     * DateTime: 2023/03/01
     * @param $scene
     * @param $scene_id
     * @param $gift_coin
     * @param $room_id
     * @param $bill_id
     * @param $to_uid
     * @param int $uid 礼物赠送者
     * @throws Exception
     */
    public function giftGroupIncomeDistribution($gift_coin, $room_id, $bill_id, $to_uid): void
    {
        $owner_uid = 100; //房主ID
        $host_uid  = 101; //主持ID
        $to_uid    = 101; //主持ID
        $datetime  = date('Y-m-d H:i:s');
        # 记录礼物收益分配
        # 房主
        $profitDistribution['owner'] = $this->coin2money($gift_coin) * (float)env('gift_module_config_extension.owner');
        # 主持人
        $profitDistribution['host'] = $this->coin2money($gift_coin) * (float)env('gift_module_config_extension.host');
        #收礼物的人
        $profitDistribution['recipient'] = $this->coin2money($gift_coin) * (float)env('gift_module_config_extension.recipient');

        ## 房主分配
        ### 增加每个受益人冻结资金余额并记录账单
        User::query()->where('id', $owner_uid)->increment('money', $profitDistribution['owner']);
        $userMoney        = User::query()->where(['id' => $owner_uid])->first(['money']);
        $userMoneyBalance = bcadd($userMoney['money'], $userMoney['money'], 8);
        $bill             = [
            'title'   => '房间礼物分成',
            'uid'     => $owner_uid,
            'number'  => $profitDistribution['money'],
            'link_id' => $bill_id,
            'balance' => $userMoneyBalance,
            'mark'    => '',
            'status'  => 0,
            'room_id' => $room_id,
        ];
        UserBillRepository::userBillIncome($bill);
        ## 主持人分配
        User::query()->where('id', $host_uid)->increment('money', $profitDistribution['host']);
        $userMoney        = User::query()->where(['id' => $host_uid])->first(['money']);
        $userMoneyBalance = bcadd($userMoney['money'], $userMoney['money'], 8);
        $bill             = [
            'title'   => '房间礼物分成',
            'uid'     => $host_uid,
            'number'  => $profitDistribution['host'],
            'link_id' => $bill_id,
            'balance' => $userMoneyBalance,
            'mark'    => '',
            'status'  => 0,
            'room_id' => $room_id,
        ];
        UserBillRepository::userBillIncome($bill);
        # 礼物接受者分成
        User::query()->where('id', $to_uid)->increment('money', $profitDistribution['recipient']);
        $userMoney        = User::query()->where(['id' => $to_uid])->first(['money']);
        $userMoneyBalance = bcadd($userMoney['money'], $userMoney['money'], 8);
        $bill             = [
            'title'   => '房间礼物分成',
            'uid'     => $to_uid,
            'number'  => $profitDistribution['recipient'],
            'link_id' => $bill_id,
            'balance' => $userMoneyBalance,
            'mark'    => '',
            'status'  => 0,
            'room_id' => $room_id,
        ];
        UserBillRepository::userBillIncome($bill);
    }

    /**
     * 币转换为现金
     * User: wml
     * DateTime: 2022/6/28 11:51
     * @param $coin
     * @param int $scale
     * @return string
     */
    public function coin2money($coin, int $scale = 2): string
    {
        $exchange_rate = (float)env('gift_module_config.exchange_rate');
        return bcmul(bcdiv($coin, 100, 8), $exchange_rate, $scale);
    }
}
