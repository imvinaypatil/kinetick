Release Notes
=============

1.0.7rc4
--------
*September 29, 2021*

- fix: incorrect margin calculation when close_position cancel

1.0.7rc2
--------
*September 23, 2021*

- instrument/fix: close_position repeated alerts + add cancel option

1.0.7rc1
--------
*September 22, 2021*

- bot/refactor: accept commands from caller
- instrument: add force option to close_position

1.0.6
--------
*September 20, 2021*

- feat: add preload active positions
- bug fixes and improvements

1.0.6rc8
--------
*September 16, 2021*

- broker/fix: pos_type getting None when non bracket order
- fix: remove symbols filter when loading positions
- refactor: add_instruments

1.0.6rc6
--------
*September 15, 2021*

- fix/zerodha: nonetype not subscriptable when zerodha retry login/order

1.0.6rc5
--------
*September 12, 2021*

- feat: add preload active positions
- bump webull v1.0.1

1.0.6rc4
--------
*September 7, 2021*

- bot: change order confirmation message text
- bot/fix: send_message updater not iterable error
- zerodha/fix: don't raise exception when session expiry hook is passed
- riskassessor: fix reset bot command

1.0.6rc3
--------
*September 6, 2021*

- docker: slim build
- riskassessor: fix reset bot command
- riskassessor: fix max allowed qty in should_trade check

1.0.0
-----------
*August 27, 2021*

- Initial release