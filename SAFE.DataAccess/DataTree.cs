using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class DataTree
    {
        IMd _head;
        Func<MdLocation, Task> _onHeadAddressChange;

        public MdLocation MdLocation => _head.MdLocation;

        public DataTree(IMd head, Func<MdLocation, Task> onHeadAddressChange)
        {
            _head = head;
            _onHeadAddressChange = onHeadAddressChange;
        }

        public async Task<Result<Pointer>> AddAsync(string key, StoredValue value)
        {
            if (_head.IsFull)
            {
                // create new head, add pointer to current head in to it.
                // the level > 0 indicates its role as pointer holder
                var newHead = await MdAccess.CreateAsync(_head.Level + 1).ConfigureAwait(false);
                var pointer = new Pointer
                {
                    MdLocation = _head.MdLocation,
                    ValueType = typeof(Pointer).Name
                };
                await newHead.AddAsync(pointer).ConfigureAwait(false);
                _head = newHead;
                await _onHeadAddressChange(newHead.MdLocation).ConfigureAwait(false);
            }

            return await _head.AddAsync(key, value).ConfigureAwait(false);
        }

        public Task<IEnumerable<StoredValue>> GetAllValuesAsync()
        {
            return _head.GetAllValuesAsync();
        }

        public Task<IEnumerable<(Pointer, StoredValue)>> GetAllPointerValuesAsync()
        {
            return _head.GetAllPointerValuesAsync();
        }
    }
}
